use zed_extension_api as zed;

struct ParquetExtension;

impl zed::Extension for ParquetExtension {
    fn new() -> Self {
        Self
    }
}

zed::register_extension!(ParquetExtension);
