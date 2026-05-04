use crate::client_topic::compression::{CodecRegistry, ErrorHandlingStrategy};
use crate::errors;
use derive_builder::Builder;
use std::sync::Arc;

fn default_codec_registry() -> Arc<CodecRegistry> {
    Arc::new(CodecRegistry::new())
}

#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicReaderOptions {
    #[builder(default = "default_codec_registry()")]
    pub(crate) codec_registry: Arc<CodecRegistry>,
    #[builder(default = "ErrorHandlingStrategy::FailFast")]
    pub(crate) compression_error_strategy: ErrorHandlingStrategy,
}
