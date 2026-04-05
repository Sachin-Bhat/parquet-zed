use anyhow::Result;
use arrow::datatypes::{DataType, Field};
use lsp_server::{Connection, Message, Response};
use lsp_types::{
    DocumentSymbol, DocumentSymbolParams, DocumentSymbolResponse, OneOf, Position, Range,
    ServerCapabilities, SymbolKind, Uri,
};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::parquet_to_arrow_schema;
use std::fs::File;
use std::path::Path;

pub fn run() -> Result<()> {
    let (connection, io_threads) = Connection::stdio();

    let capabilities = ServerCapabilities {
        document_symbol_provider: Some(OneOf::Left(true)),
        ..Default::default()
    };
    connection.initialize(serde_json::to_value(capabilities)?)?;

    for msg in &connection.receiver {
        let Message::Request(req) = msg else {
            continue;
        };
        if connection.handle_shutdown(&req)? {
            break;
        }
        if req.method == "textDocument/documentSymbol" {
            let params: DocumentSymbolParams = serde_json::from_value(req.params.clone())?;
            let symbols = symbols_for_uri(&params.text_document.uri).unwrap_or_default();
            let resp = Response::new_ok(
                req.id,
                serde_json::to_value(DocumentSymbolResponse::Nested(symbols))?,
            );
            connection.sender.send(Message::Response(resp))?;
        }
    }

    io_threads.join()?;
    Ok(())
}

fn symbols_for_uri(uri: &Uri) -> Result<Vec<DocumentSymbol>> {
    let path = uri.path().as_str();
    symbols_for_path(Path::new(path))
}

fn symbols_for_path(path: &Path) -> Result<Vec<DocumentSymbol>> {
    let file = File::open(path)?;
    let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())?;
    let file_meta = metadata.metadata().file_metadata();
    let schema =
        parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())?;

    Ok(schema.fields().iter().map(|f| field_to_symbol(f)).collect())
}

fn field_to_symbol(field: &Field) -> DocumentSymbol {
    let zero = Position {
        line: 0,
        character: 0,
    };
    let range = Range {
        start: zero,
        end: zero,
    };

    let (kind, children) = match field.data_type() {
        DataType::Struct(fields) => {
            let children: Vec<DocumentSymbol> =
                fields.iter().map(|f| field_to_symbol(f)).collect();
            (SymbolKind::STRUCT, Some(children))
        }
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            (SymbolKind::ARRAY, Some(vec![field_to_symbol(f)]))
        }
        DataType::Boolean => (SymbolKind::BOOLEAN, None),
        DataType::Utf8 | DataType::LargeUtf8 => (SymbolKind::STRING, None),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => (SymbolKind::NUMBER, None),
        _ => (SymbolKind::FIELD, None),
    };

    #[allow(deprecated)]
    DocumentSymbol {
        name: field.name().clone(),
        detail: Some(field.data_type().to_string()),
        kind,
        tags: None,
        deprecated: None,
        range,
        selection_range: range,
        children,
    }
}
