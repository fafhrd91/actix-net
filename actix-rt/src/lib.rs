//! Tokio-based single-threaded async runtime for the Actix ecosystem.
//!
//! In most parts of the the Actix ecosystem, it has been chosen to use !Send futures. For this
//! reason, a single-threaded runtime is appropriate since it is guaranteed that futures will not
//! be moved between threads. This can result in small performance improvements over cases where
//! atomics would otherwise be needed.
//!
//! To achieve similar performance to multi-threaded, work-stealing runtimes, applications
//! using `actix-rt` will create multiple, mostly disconnected, single-threaded runtimes.
//! This approach has good performance characteristics for workloads where the majority of tasks
//! have similar runtime expense.
//!
//! The disadvantage is that idle threads will not steal work from very busy, stuck or otherwise
//! backlogged threads. Tasks that are disproportionately expensive should be offloaded to the
//! blocking task thread-pool using [`task::spawn_blocking`].
//!
//! # Examples
//! ```
//! use std::sync::mpsc;
//! use actix_rt::{Arbiter, System};
//!
//! let _ = System::new();
//!
//! let (tx, rx) = mpsc::channel::<u32>();
//!
//! let arbiter = Arbiter::new();
//! arbiter.spawn_fn(move || tx.send(42).unwrap());
//!
//! let num = rx.recv().unwrap();
//! assert_eq!(num, 42);
//!
//! arbiter.stop();
//! arbiter.join().unwrap();
//! ```

#![deny(rust_2018_idioms, nonstandard_style)]
#![allow(clippy::type_complexity)]
#![warn(missing_docs)]
#![doc(html_logo_url = "https://actix.rs/img/logo.png")]
#![doc(html_favicon_url = "https://actix.rs/favicon.ico")]

use std::future::Future;

use tokio::task::JoinHandle;

// Cannot define a main macro when compiled into test harness.
// Workaround for https://github.com/rust-lang/rust/issues/62127.
#[cfg(all(feature = "macros", not(test)))]
pub use actix_macros::{main, test};

mod arbiter;
mod runtime;
mod system;

pub use self::arbiter::{Arbiter, ArbiterHandle};
pub use self::runtime::Runtime;
pub use self::system::{System, SystemRunner};

pub use tokio::pin;

pub mod signal {
    //! Asynchronous signal handling (Tokio re-exports).

    #[cfg(unix)]
    pub mod unix {
        //! Unix specific signals (Tokio re-exports).
        pub use tokio::signal::unix::*;
    }
    pub use tokio::signal::ctrl_c;
}

pub mod net {
    //! TCP/UDP/Unix bindings (mostly Tokio re-exports).

    use std::{
        future::Future,
        io,
        task::{Context, Poll},
    };

    pub use tokio::io::Ready;
    use tokio::io::{AsyncRead, AsyncWrite, Interest};
    pub use tokio::net::UdpSocket;
    pub use tokio::net::{TcpListener, TcpSocket, TcpStream};

    #[cfg(unix)]
    pub use tokio::net::{UnixDatagram, UnixListener, UnixStream};

    /// Extension trait over async read+write types that can also signal readiness.
    #[doc(hidden)]
    pub trait ActixStream: AsyncRead + AsyncWrite + Unpin {
        /// Poll stream and check read readiness of Self.
        ///
        /// See [tokio::net::TcpStream::poll_read_ready] for detail on intended use.
        fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<Ready>>;

        /// Poll stream and check write readiness of Self.
        ///
        /// See [tokio::net::TcpStream::poll_write_ready] for detail on intended use.
        fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<Ready>>;
    }

    impl ActixStream for TcpStream {
        fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<Ready>> {
            let ready = self.ready(Interest::READABLE);
            tokio::pin!(ready);
            ready.poll(cx)
        }

        fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<Ready>> {
            let ready = self.ready(Interest::WRITABLE);
            tokio::pin!(ready);
            ready.poll(cx)
        }
    }

    #[cfg(unix)]
    impl ActixStream for UnixStream {
        fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<Ready>> {
            let ready = self.ready(Interest::READABLE);
            tokio::pin!(ready);
            ready.poll(cx)
        }

        fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<Ready>> {
            let ready = self.ready(Interest::WRITABLE);
            tokio::pin!(ready);
            ready.poll(cx)
        }
    }

    impl<Io: ActixStream + ?Sized> ActixStream for Box<Io> {
        fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<Ready>> {
            (**self).poll_read_ready(cx)
        }

        fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<Ready>> {
            (**self).poll_write_ready(cx)
        }
    }
}

pub mod time {
    //! Utilities for tracking time (Tokio re-exports).

    pub use tokio::time::Instant;
    pub use tokio::time::{interval, interval_at, Interval};
    pub use tokio::time::{sleep, sleep_until, Sleep};
    pub use tokio::time::{timeout, Timeout};
}

pub mod task {
    //! Task management (Tokio re-exports).

    pub use tokio::task::{spawn_blocking, yield_now, JoinError, JoinHandle};
}

/// Spawns a future on the current thread.
///
/// # Panics
/// Panics if Actix system is not running.
#[inline]
pub fn spawn<Fut>(f: Fut) -> JoinHandle<Fut::Output>
where
    Fut: Future + 'static,
    Fut::Output: 'static,
{
    tokio::task::spawn_local(f)
}
