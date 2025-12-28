use clap::{Parser, ValueEnum};
use csv::Writer;
use google_sheets4::Sheets;
use google_sheets4::hyper_rustls::HttpsConnectorBuilder;
use google_sheets4::hyper_util::client::legacy::Client;
use google_sheets4::hyper_util::rt::TokioExecutor;
use google_sheets4::yup_oauth2;
use google_sheets4::yup_oauth2::ServiceAccountAuthenticator;
use serde_json::Value;
use thiserror::Error;

use std::io;
use std::path::PathBuf;

#[derive(Debug, Error)]
enum ParseError {
    #[error(
        "row {row}, col {col} ('{name}'): type does not match. expected: {expected}, actual: {actual}"
    )]
    TypeMismatch {
        row: usize,
        col: usize,
        name: String,
        expected: &'static str,
        actual: String,
    },

    #[error("row {row}: required '{name}' (index: {col})")]
    MissingColumn {
        row: usize,
        col: usize,
        name: String,
    },

    #[error("row {row}, col {col}: validation error: {message}")]
    ValidationError {
        row: usize,
        col: usize,
        message: String,
    },
}

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    Parse(#[from] ParseError),

    #[error(transparent)]
    Api(#[from] google_sheets4::Error),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    WriteError(#[from] csv::Error),
}

#[derive(ValueEnum, Clone, Debug)]
enum OnError {
    Fail,
    Skip,
    Log,
}

#[derive(Debug, Clone)]
enum DataType {
    String,
    Integer,
    Float,
    Boolean,
}

#[derive(Debug, Clone)]
enum DataValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

impl DataValue {
    fn to_csv_string(&self) -> String {
        match self {
            DataValue::Integer(v) => v.to_string(),
            DataValue::Float(v) => v.to_string(),
            DataValue::String(v) => v.clone(),
            DataValue::Boolean(v) => v.to_string(),
            DataValue::Null => "".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct Record(Vec<DataValue>);

impl Record {
    fn iter(&self) -> impl Iterator<Item = &DataValue> {
        self.0.iter()
    }
}

struct Column {
    name: String,
    data_type: DataType,
    required: bool,
}

struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    fn parse_row(&self, row_index: usize, raw_row: Vec<Value>) -> Result<Record, ParseError> {
        let mut processed = Vec::new();

        for (i, col) in self.columns.iter().enumerate() {
            let raw_val = raw_row.get(i).unwrap_or(&Value::Null);

            if let Value::Null = raw_val {
                if col.required {
                    return Err(ParseError::MissingColumn {
                        row: row_index,
                        col: i,
                        name: col.name.clone(),
                    });
                }
                processed.push(DataValue::Null);
                continue;
            }

            let value = match col.data_type {
                DataType::String => DataValue::String(raw_val.as_str().unwrap_or("").to_string()),
                DataType::Integer => {
                    let s = raw_val.as_str().unwrap_or("0");
                    s.parse::<i64>().map(DataValue::Integer).map_err(|_| {
                        ParseError::TypeMismatch {
                            row: row_index,
                            col: i,
                            name: col.name.clone(),
                            expected: "Integer",
                            actual: s.to_string(),
                        }
                    })?
                }
                DataType::Float => {
                    let s = raw_val.as_str().unwrap_or("0.0");
                    s.parse::<f64>().map(DataValue::Float).map_err(|_| {
                        ParseError::TypeMismatch {
                            row: row_index,
                            col: i,
                            name: col.name.clone(),
                            expected: "Float",
                            actual: s.to_string(),
                        }
                    })?
                }
                DataType::Boolean => {
                    let s = raw_val.as_str().unwrap_or("false");
                    s.parse::<bool>().map(DataValue::Boolean).map_err(|_| {
                        ParseError::TypeMismatch {
                            row: row_index,
                            col: i,
                            name: col.name.clone(),
                            expected: "Boolean",
                            actual: s.to_string(),
                        }
                    })?
                }
            };
            processed.push(value);
        }
        Ok(Record(processed))
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about=None)]
#[command(propagate_version = true)]
struct Cli {
    #[clap(short, long)]
    sheet_id: String,

    #[clap(short, long, default_value = "Sheet1!A1:Z100")]
    range: String,

    #[arg(long, default_value_t = false)]
    has_header: bool,

    #[arg(long, default_value_t = false)]
    output_header: bool,

    #[arg(long, value_enum, default_value_t = OnError::Log)]
    on_error: OnError,

    #[clap(long)]
    service_account_file: Option<PathBuf>,
}

fn generate_default_schema(columns: usize) -> Schema {
    let columns = (0..columns)
        .map(|c| Column {
            name: format!("#{}", c),
            data_type: DataType::String,
            required: false,
        })
        .collect();
    Schema { columns }
}

fn generate_schema(header: &[Value]) -> Schema {
    let mut columns = vec![];
    for column in header {
        columns.push(Column {
            name: column.to_string().trim_matches('"').to_string(),
            data_type: DataType::String,
            required: false,
        });
    }
    Schema { columns }
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let cli = Cli::parse();

    let mut wtr = Writer::from_writer(io::stdout());

    let service_account_file = cli
        .service_account_file
        .expect("service account file is required");
    let creds = yup_oauth2::read_service_account_key(service_account_file).await?;
    let auth = ServiceAccountAuthenticator::builder(creds).build().await?;
    let hub = Sheets::new(
        Client::builder(TokioExecutor::new()).build(
            HttpsConnectorBuilder::new()
                .with_native_roots()?
                .https_or_http()
                .enable_http1()
                .build(),
        ),
        auth,
    );

    let result = hub
        .spreadsheets()
        .values_get(&cli.sheet_id, &cli.range)
        .doit()
        .await?;

    let (_, value_range) = result;
    if let Some(values) = value_range.values {
        let mut iter = values.into_iter().enumerate().peekable();

        if let Some((_, header)) = iter.peek() {
            let schema;
            if cli.has_header {
                schema = generate_schema(header);
                iter.next();
            } else {
                schema = generate_default_schema(header.len());
            };
            if cli.output_header {
                wtr.write_record(schema.columns.iter().map(|c| &c.name))?;
            }

            for (i, raw_row) in iter {
                match schema.parse_row(i, raw_row) {
                    Ok(record) => {
                        let csv_row: Vec<String> =
                            record.iter().map(|v| v.to_csv_string()).collect();
                        wtr.write_record(csv_row)?;
                    }
                    Err(e) => match cli.on_error {
                        OnError::Fail => return Err(e.into()),
                        OnError::Skip => continue,
                        OnError::Log => eprintln!("{:?}", e),
                    },
                }
            }
        } else {
            eprintln!("data not found");
        }
    } else {
        eprintln!("data not found");
    }
    wtr.flush()?;

    Ok(())
}
