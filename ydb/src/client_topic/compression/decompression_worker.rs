use super::ordered_task_queue::OrderedTaskQueue;
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::error_strategy::ErrorHandlingStrategy;
use crate::client_topic::list_types::Codec;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawBatchWithId;
use crate::YdbResult;
use prost::bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct DecompressionWorker {
    codec_registry: Arc<CodecRegistry>,
    error_strategy: ErrorHandlingStrategy,
    queue: OrderedTaskQueue<RawBatchWithId>,
}

impl DecompressionWorker {
    pub fn new(
        codec_registry: Arc<CodecRegistry>,
        error_strategy: ErrorHandlingStrategy,
    ) -> (Self, mpsc::UnboundedReceiver<YdbResult<RawBatchWithId>>) {
        let (queue, receiver) = OrderedTaskQueue::new();
        (
            Self {
                codec_registry,
                error_strategy,
                queue,
            },
            receiver,
        )
    }

    pub fn process_batch(&self, batch: RawBatchWithId) -> YdbResult<()> {
        let registry = self.codec_registry.clone();
        let strategy = self.error_strategy.clone();
        self.queue.submit(Box::new(move || {
            decompress_batch(batch, &registry, &strategy)
        }))
    }
}

fn decompress_batch(
    mut batch_with_id: RawBatchWithId,
    registry: &CodecRegistry,
    strategy: &ErrorHandlingStrategy,
) -> YdbResult<RawBatchWithId> {
    let batch = &mut batch_with_id.batch;
    let codec = Codec {
        code: batch.codec.code,
    };
    if codec == Codec::RAW {
        return Ok(batch_with_id);
    }

    for message in batch.message_data.iter_mut() {
        match registry.decompress(Bytes::copy_from_slice(&message.data), &codec) {
            Ok(decompressed) => {
                message.data = decompressed.to_vec();
            }
            Err(error) => match strategy {
                ErrorHandlingStrategy::FailFast => {
                    return Err(error);
                }
                ErrorHandlingStrategy::Skip => {}
            },
        }
    }

    Ok(batch_with_id)
}
