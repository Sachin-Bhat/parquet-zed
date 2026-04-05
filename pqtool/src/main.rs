use std::fs::File;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::parquet_to_arrow_schema;
use parquet::basic::TimeUnit as ParquetTimeUnit;
use parquet::schema::types::ColumnDescPtr;

#[derive(Parser)]
#[command(name = "pqtool", version, about = "Parquet utility CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Inspect parquet file metadata and schema.
    Inspect { path: PathBuf },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Inspect { path } => inspect(path),
    }
}

fn inspect(path: PathBuf) -> Result<()> {
    let file = File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
    let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())
        .with_context(|| format!("failed to read parquet metadata for {}", path.display()))?;

    let file_meta = metadata.metadata().file_metadata();
    let arrow_schema =
        parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
            .context("failed converting parquet schema to arrow schema")?;

    println!("File: {}", path.display());
    println!("Rows: {}", file_meta.num_rows());
    println!("Row groups: {}", metadata.metadata().num_row_groups());
    println!("Parquet version: {}", file_meta.version());
    if let Some(created_by) = file_meta.created_by() {
        println!("Created by: {created_by}");
    }
    if let Some(kv) = file_meta.key_value_metadata()
        && !kv.is_empty()
    {
        println!("Key-value metadata:");
        for item in kv {
            let value = item.value.as_deref().unwrap_or("<null>");
            println!("  {} = {}", item.key, value);
        }
    }
    println!("Columns:");

    let columns = file_meta.schema_descr().columns();
    let fields = arrow_schema.fields();
    for (field, column) in fields.iter().zip(columns.iter()) {
        let field = field.as_ref();
        let type_str = format_type(field.data_type().to_string(), column);
        println!(
            "- {}: {} nullable={}",
            field.name(),
            type_str,
            field.is_nullable()
        );
    }
    if fields.len() > columns.len() {
        for field in fields.iter().skip(columns.len()) {
            let field = field.as_ref();
            println!(
                "- {}: {} nullable={}",
                field.name(),
                field.data_type(),
                field.is_nullable()
            );
        }
        println!("Note: non-flat/nested schema detected; extra fields shown in Arrow form.");
    } else if columns.len() > fields.len() {
        println!(
            "Note: non-flat/nested schema detected; some Parquet leaf columns do not map 1:1 to displayed Arrow fields."
        );
    }

    Ok(())
}

fn format_type(default: String, column: &ColumnDescPtr) -> String {
    if let Some(logical) = column.logical_type_ref()
        && let parquet::basic::LogicalType::Timestamp { unit, .. } = logical
    {
        let unit = match unit {
            ParquetTimeUnit::MILLIS => "Milliseconds",
            ParquetTimeUnit::MICROS => "Microseconds",
            ParquetTimeUnit::NANOS => "Nanoseconds",
        };
        return format!("Timestamp({unit})");
    }
    default
}
