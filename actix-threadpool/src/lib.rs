//! Thread pool for blocking operations
//!
//! The pool would lazily generate thread according to the workload and spawn up to a total amount
//! of you machine's `logical CPU cores * 5` threads. Any spawned threads kept idle for 1 minutes
//! would be recycled and de spawned.
//!
//! *. Settings are configuable through env variables.
//!
//! # Example:
//! ```rust
//! #[actix_rt::main]
//! async fn main() {
//!     // Optional: Set the max thread count for the blocking pool.
//!     std::env::set_var("ACTIX_THREADPOOL", "30");
//!     // Optional: Set the min thread count for the blocking pool.
//!     std::env::set_var("ACTIX_THREADPOOL_MIN", "1");
//!     // Optional: Set the timeout duration IN SECONDS for the blocking pool's idle threads.
//!     std::env::set_var("ACTIX_THREADPOOL_TIMEOUT", "60");
//!
//!     let future = actix_threadpool::run(|| {
//!         /* Some blocking code with a Result<T, E> as return type */
//!         Ok::<usize, ()>(1usize)
//!     });
//!
//!     // calling actix_web::web::block(|| {}) would have the same functionality.
//!
//!     /*
//!         We can await on this blocking code and NOT block our runtime.
//!         When we waiting our actix runtime can switch to other async tasks.
//!     */
//!
//!     let result: Result<usize, actix_threadpool::BlockingError<()>> = future.await;
//!
//!     assert_eq!(1usize, result.unwrap())
//! }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use derive_more::Display;
use futures_channel::oneshot;
use jian_rs::ThreadPool;
use parking_lot::Mutex;

/// Env variable for default cpu pool max size.
const ENV_MAX_THREADS: &str = "ACTIX_THREADPOOL";

/// Env variable for default cpu pool min size.
const ENV_MIN_THREADS: &str = "ACTIX_THREADPOOL_MIN";

/// Env variable for default thread idle timeout duration.
const ENV_IDLE_TIMEOUT: &str = "ACTIX_THREADPOOL_TIMEOUT";

lazy_static::lazy_static! {
    pub(crate) static ref POOL: Mutex<ThreadPool> = {
        let max = parse_env(ENV_MAX_THREADS).unwrap_or_else(|| num_cpus::get() * 5);
        let min = parse_env(ENV_MIN_THREADS).unwrap_or(1);
        let dur = parse_env(ENV_IDLE_TIMEOUT).unwrap_or(60);

        Mutex::new(ThreadPool::builder()
            .thread_name("actix-threadpool")
            .max_threads(max)
            .min_threads(min)
            .idle_timeout(Duration::from_secs(dur))
            .build())
    };
}

thread_local! {
    static POOL_LOCAL: ThreadPool = {
        POOL.lock().clone()
    }
}

fn parse_env<R: std::str::FromStr>(env: &str) -> Option<R> {
    std::env::var(env).ok().and_then(|val| {
        val.parse()
            .map_err(|_| log::warn!("Can not parse {} value, using default", env))
            .ok()
    })
}

/// Blocking operation execution error
#[derive(Debug, Display)]
pub enum BlockingError<E: fmt::Debug> {
    #[display(fmt = "{:?}", _0)]
    Error(E),
    #[display(fmt = "Thread pool is gone")]
    Canceled,
}

impl<E: fmt::Debug> std::error::Error for BlockingError<E> {}

/// Execute blocking function on a thread pool, returns future that resolves
/// to result of the function execution.
pub fn run<F, I, E>(f: F) -> CpuFuture<I, E>
where
    F: FnOnce() -> Result<I, E> + Send + 'static,
    I: Send + 'static,
    E: Send + fmt::Debug + 'static,
{
    let (tx, rx) = oneshot::channel();

    POOL_LOCAL.with(|pool| {
        let _ = pool.execute(move || {
            let _ = tx.send(f());
        });
    });

    CpuFuture { rx }
}

/// Blocking operation completion future. It resolves with results
/// of blocking function execution.
pub struct CpuFuture<I, E> {
    rx: oneshot::Receiver<Result<I, E>>,
}

impl<I, E: fmt::Debug> Future for CpuFuture<I, E> {
    type Output = Result<I, BlockingError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let rx = Pin::new(&mut self.rx);
        let res = match rx.poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(res) => res
                .map_err(|_| BlockingError::Canceled)
                .and_then(|res| res.map_err(BlockingError::Error)),
        };
        Poll::Ready(res)
    }
}
