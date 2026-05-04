use crate::{YdbError, YdbResult};
use tokio::sync::mpsc;

type WorkerTask<T> = Box<dyn FnOnce() -> YdbResult<T> + Send + 'static>;
pub struct OrderedTaskQueue<T> {
    task_sender: mpsc::UnboundedSender<WorkerTask<T>>,
}

impl<T: Send + 'static> OrderedTaskQueue<T> {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<YdbResult<T>>) {
        let (task_sender, mut task_receiver) = mpsc::unbounded_channel::<WorkerTask<T>>();
        let (result_sender, result_receiver) = mpsc::unbounded_channel::<YdbResult<T>>();

        tokio::task::spawn_blocking(move || {
            while let Some(task) = task_receiver.blocking_recv() {
                let result = task();
                if result_sender.send(result).is_err() {
                    break;
                }
            }
        });

        (Self { task_sender }, result_receiver)
    }

    pub fn submit(&self, task: WorkerTask<T>) -> YdbResult<()> {
        self.task_sender
            .send(task)
            .map_err(|err| YdbError::custom(format!("ordered queue is closed: {err}")))
    }
}
