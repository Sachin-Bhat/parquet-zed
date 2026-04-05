# parquet-zed

A Zed extension for viewing and editing Apache Parquet files.

## Features

- **Schema outline** — document symbols show the file's column structure in Zed's outline panel
- **Preview** — render a parquet file as a markdown table
- **Edit** — convert to JSONL for editing, auto-sync back to parquet on save

## Limitations (Zed)

Zed does not support custom editors for binary files. Opening a `.parquet` file directly will show **"Binary files are not supported"** — this is a Zed limitation, not a bug in this extension. An official API for custom file viewers is tracked at [zed-industries/zed#17325](https://github.com/zed-industries/zed/issues/17325).

Additionally, language-specific tasks contributed by extensions are currently unreliable in Zed ([zed-industries/zed#32465](https://github.com/zed-industries/zed/issues/32465)), so the edit/save tasks may not appear in the task runner.

## Workaround: editing parquet files

Use `pqtool` from Zed's integrated terminal (`ctrl+backtick`).

### Install pqtool

```bash
cargo install --path pqtool
```

### Edit a parquet file

```bash
# 1. Convert to JSONL and open in Zed
pqtool to-jsonl path/to/file.parquet
zed path/to/file.parquet.jsonl

# 2. In a second terminal, start the file watcher
#    It converts the JSONL back to parquet on every save
pqtool watch path/to/file.parquet.jsonl
```

Edit the `.parquet.jsonl` file normally in Zed. Each time you save (`Ctrl+S`), the watcher writes the changes back to the original `.parquet` file. Press `Ctrl+C` to stop watching when done.

### Preview a parquet file

```bash
pqtool preview path/to/file.parquet
```

### Inspect schema and metadata

```bash
pqtool inspect path/to/file.parquet
```

## pqtool reference

```
pqtool inspect <file>                  Show schema, row count, metadata
pqtool preview <file> [--rows N]       Print first N rows as a markdown table (default 50)
pqtool to-jsonl <file> [output]        Convert parquet to JSONL (default: <file>.jsonl)
pqtool from-jsonl <input> <output>     Convert JSONL back to parquet
pqtool watch <file.parquet.jsonl>      Watch JSONL file, auto-save to parquet on each write
```

## Type fidelity on roundtrip

JSON has no integer width or timestamp types. When converting JSONL back to parquet, Arrow infers types from the data:

- All integers become `Int64`
- All floats become `Float64`
- Timestamps serialised as ISO strings become `Utf8` unless the original schema is re-applied manually

For read-heavy workflows where exact types matter, use `pqtool inspect` to compare the schema before and after editing.
