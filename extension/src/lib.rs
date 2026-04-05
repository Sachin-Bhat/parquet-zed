use zed_extension_api as zed;

struct ParquetExtension;

impl zed::Extension for ParquetExtension {
    fn new() -> Self {
        Self
    }

    fn language_server_command(
        &mut self,
        _language_server_id: &zed::LanguageServerId,
        _worktree: &zed::Worktree,
    ) -> zed::Result<zed::Command> {
        Ok(zed::Command {
            command: "pqtool".into(),
            args: vec!["lsp".into()],
            env: Default::default(),
        })
    }
}

zed::register_extension!(ParquetExtension);
