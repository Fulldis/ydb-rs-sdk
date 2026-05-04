use super::ordered_task_queue::OrderedTaskQueue;
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::error_strategy::ErrorHandlingStrategy;
use crate::client_topic::list_types::Codec;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::YdbResult;
use prost::bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct CompressionWorker {
    codec: Option<Codec>,
    codec_registry: Arc<CodecRegistry>,
    error_strategy: ErrorHandlingStrategy,
    queue: OrderedTaskQueue<Vec<TopicWriterMessage>>,
}

impl CompressionWorker {
    pub fn new(
        codec: Option<Codec>,
        codec_registry: Arc<CodecRegistry>,
        error_strategy: ErrorHandlingStrategy,
    ) -> (
        Self,
        mpsc::UnboundedReceiver<YdbResult<Vec<TopicWriterMessage>>>,
    ) {
        let (queue, receiver) = OrderedTaskQueue::new();
        (
            Self {
                codec,
                codec_registry,
                error_strategy,
                queue,
            },
            receiver,
        )
    }

    pub fn process_batch(&self, batch: Vec<TopicWriterMessage>) -> YdbResult<()> {
        let registry = self.codec_registry.clone();
        let strategy = self.error_strategy.clone();
        let codec = self.codec.clone();

        self.queue.submit(Box::new(move || {
            compress_batch(batch, &registry, &codec, &strategy)
        }))
    }
}

fn compress_batch(
    mut batch: Vec<TopicWriterMessage>,
    registry: &CodecRegistry,
    codec: &Option<Codec>,
    strategy: &ErrorHandlingStrategy,
) -> YdbResult<Vec<TopicWriterMessage>> {
    let codec = match codec {
        None => return Ok(batch),
        Some(c) => c,
    };

    for message in batch.iter_mut() {
        match registry.compress(Bytes::from(message.data.clone()), codec) {
            Ok(compressed) => message.data = compressed.to_vec(),
            Err(err) => match strategy {
                ErrorHandlingStrategy::FailFast => return Err(err),
                ErrorHandlingStrategy::Skip => {}
            },
        }
    }

    Ok(batch)
}
