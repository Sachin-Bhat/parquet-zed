mod lsp;

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use clap::{Parser, Subcommand};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
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
    /// Preview parquet file as a markdown table.
    Preview {
        path: PathBuf,
        /// Maximum number of rows to show.
        #[arg(long, default_value_t = 50)]
        rows: usize,
    },
    /// Convert parquet to JSONL for editing.
    ToJsonl {
        path: PathBuf,
        /// Output path (defaults to <path>.jsonl).
        output: Option<PathBuf>,
    },
    /// Convert JSONL back to parquet.
    FromJsonl {
        input: PathBuf,
        output: PathBuf,
    },
    /// Watch a .parquet.jsonl file and auto-save back to parquet on each write.
    Watch { path: PathBuf },
    /// Start the LSP server (communicates over stdin/stdout).
    Lsp,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Inspect { path } => inspect(path),
        Commands::Preview { path, rows } => preview(path, rows),
        Commands::ToJsonl { path, output } => to_jsonl(path, output),
        Commands::FromJsonl { input, output } => from_jsonl(input, output),
        Commands::Watch { path } => watch(path),
        Commands::Lsp => lsp::run(),
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

fn preview(path: PathBuf, max_rows: usize) -> Result<()> {
    let file = File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
    let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())
        .with_context(|| format!("failed to read parquet metadata for {}", path.display()))?;

    let file_meta = metadata.metadata().file_metadata();
    let arrow_schema =
        parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
            .context("failed converting parquet schema to arrow schema")?;

    let filename = path.file_name().unwrap_or(path.as_os_str()).to_string_lossy();
    println!("## {filename}");
    println!();
    println!(
        "**Rows:** {} | **Row groups:** {} | **Version:** {}",
        file_meta.num_rows(),
        metadata.metadata().num_row_groups(),
        file_meta.version(),
    );
    println!();

    // Schema table
    println!("### Schema");
    println!();
    println!("| Column | Type | Nullable |");
    println!("|--------|------|----------|");
    let columns = file_meta.schema_descr().columns();
    let fields = arrow_schema.fields();
    for (field, column) in fields.iter().zip(columns.iter()) {
        let type_str = format_type(field.data_type().to_string(), column);
        println!("| {} | {} | {} |", field.name(), type_str, field.is_nullable());
    }
    println!();

    // Data preview
    let file = File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("failed to create parquet reader")?
        .with_batch_size(max_rows);
    let mut reader = builder.build().context("failed to build parquet reader")?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut total = 0usize;
    for batch in reader.by_ref() {
        let batch = batch.context("failed to read record batch")?;
        let remaining = max_rows - total;
        let batch = batch.slice(0, batch.num_rows().min(remaining));
        total += batch.num_rows();
        batches.push(batch);
        if total >= max_rows {
            break;
        }
    }

    let shown = total.min(max_rows);
    println!("### Preview (first {shown} rows)");
    println!();

    if batches.is_empty() {
        println!("*(no data)*");
        return Ok(());
    }

    // Header row
    let schema = batches[0].schema();
    let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    println!("| {} |", headers.join(" | "));
    println!("|{}|", headers.iter().map(|_| "---").collect::<Vec<_>>().join("|"));

    // Data rows
    let options = arrow::util::display::FormatOptions::default().with_null("null");
    for batch in &batches {
        let formatters: Vec<_> = (0..batch.num_columns())
            .map(|col| arrow::util::display::ArrayFormatter::try_new(batch.column(col), &options))
            .collect::<Result<_, _>>()
            .context("failed to create array formatters")?;
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = formatters
                .iter()
                .map(|f| f.value(row).to_string().replace('|', "\\|"))
                .collect();
            println!("| {} |", cells.join(" | "));
        }
    }

    Ok(())
}

fn watch(path: PathBuf) -> Result<()> {
    use notify::{EventKind, RecursiveMode, Watcher, recommended_watcher};
    use std::sync::mpsc;

    let parquet_path = {
        let s = path.as_os_str();
        let p = PathBuf::from(s);
        // strip .jsonl suffix
        let name = p.to_string_lossy();
        if let Some(stripped) = name.strip_suffix(".jsonl") {
            PathBuf::from(stripped)
        } else {
            anyhow::bail!("expected a .jsonl file, got {}", path.display());
        }
    };

    eprintln!(
        "watching {} → {}",
        path.display(),
        parquet_path.display()
    );
    eprintln!("press Ctrl+C to stop");

    let (tx, rx) = mpsc::channel();
    let mut watcher = recommended_watcher(move |res| {
        let _ = tx.send(res);
    })?;
    watcher.watch(&path, RecursiveMode::NonRecursive)?;

    for event in rx {
        let event = event?;
        if matches!(
            event.kind,
            EventKind::Modify(_) | EventKind::Create(_)
        ) {
            match from_jsonl(path.clone(), parquet_path.clone()) {
                Ok(()) => {}
                Err(e) => eprintln!("error: {e:#}"),
            }
        }
    }
    Ok(())
}

fn to_jsonl(path: PathBuf, output: Option<PathBuf>) -> Result<()> {
    let out_path = output.unwrap_or_else(|| {
        let mut s = path.clone().into_os_string();
        s.push(".jsonl");
        PathBuf::from(s)
    });

    let file = File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("failed to create parquet reader")?
        .build()
        .context("failed to build parquet reader")?;

    let out = File::create(&out_path)
        .with_context(|| format!("failed to create {}", out_path.display()))?;
    let mut writer = arrow_json::writer::LineDelimitedWriter::new(out);
    for batch in reader {
        writer.write(&batch.context("failed to read batch")?)?;
    }
    writer.finish().context("failed to finalize JSONL")?;

    println!("{}", out_path.display());
    Ok(())
}

fn from_jsonl(input: PathBuf, output: PathBuf) -> Result<()> {
    let (schema, _) = arrow_json::reader::infer_json_schema(
        BufReader::new(File::open(&input).with_context(|| format!("failed to open {}", input.display()))?),
        None,
    )
    .context("failed to infer schema from JSONL")?;
    let schema = Arc::new(schema);

    let reader = arrow_json::ReaderBuilder::new(schema.clone())
        .build(BufReader::new(
            File::open(&input).with_context(|| format!("failed to open {}", input.display()))?,
        ))
        .context("failed to build JSONL reader")?;

    let out = File::create(&output)
        .with_context(|| format!("failed to create {}", output.display()))?;
    let mut writer =
        ArrowWriter::try_new(out, schema, None).context("failed to create parquet writer")?;
    for batch in reader {
        writer.write(&batch.context("failed to read JSONL batch")?)?;
    }
    writer.close().context("failed to finalize parquet")?;

    eprintln!("saved {}", output.display());
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
