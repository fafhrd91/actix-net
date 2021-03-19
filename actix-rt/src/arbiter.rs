use std::{
    cell::RefCell,
    fmt,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
    thread,
};

use futures_core::ready;
use futures_intrusive::channel::shared::{
    oneshot_broadcast_channel, OneshotBroadcastReceiver, OneshotBroadcastSender,
};
use tokio::{sync::mpsc, task::LocalSet};

use crate::{
    runtime::{default_tokio_runtime, Runtime},
    system::{System, SystemCommand},
};

pub(crate) static COUNT: AtomicUsize = AtomicUsize::new(0);

thread_local!(
    static HANDLE: RefCell<Option<ArbiterHandle>> = RefCell::new(None);
);

pub(crate) enum ArbiterCommand {
    Stop,
    Execute(Pin<Box<dyn Future<Output = ()> + Send>>),
}

impl fmt::Debug for ArbiterCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArbiterCommand::Stop => write!(f, "ArbiterCommand::Stop"),
            ArbiterCommand::Execute(_) => write!(f, "ArbiterCommand::Execute"),
        }
    }
}

/// A handle for sending spawn and stop messages to an [Arbiter].
#[derive(Debug, Clone)]
pub struct ArbiterHandle {
    tx: mpsc::UnboundedSender<ArbiterCommand>,
    /// Is `None` for system arbiter.
    stopped_rx: Option<OneshotBroadcastReceiver<()>>,
}

impl ArbiterHandle {
    pub(crate) fn new(
        tx: mpsc::UnboundedSender<ArbiterCommand>,
        stopped_rx: OneshotBroadcastReceiver<()>,
    ) -> Self {
        Self {
            tx,
            stopped_rx: Some(stopped_rx),
        }
    }

    pub(crate) fn for_system(tx: mpsc::UnboundedSender<ArbiterCommand>) -> Self {
        Self {
            tx,
            stopped_rx: None,
        }
    }

    /// Send a future to the [Arbiter]'s thread and spawn it.
    ///
    /// If you require a result, include a response channel in the future.
    ///
    /// Returns true if future was sent successfully and false if the [Arbiter] has died.
    pub fn spawn<Fut>(&self, future: Fut) -> bool
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.tx
            .send(ArbiterCommand::Execute(Box::pin(future)))
            .is_ok()
    }

    /// Send a function to the [Arbiter]'s thread and execute it.
    ///
    /// Any result from the function is discarded. If you require a result, include a response
    /// channel in the function.
    ///
    /// Returns true if function was sent successfully and false if the [Arbiter] has died.
    pub fn spawn_fn<F>(&self, f: F) -> bool
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawn(async { f() })
    }

    /// Instruct [Arbiter] to stop processing it's event loop.
    ///
    /// Returns true if stop message was sent successfully and false if the [Arbiter] has
    /// been dropped.
    pub fn stop(&self) -> bool {
        self.tx.send(ArbiterCommand::Stop).is_ok()
    }

    /// Will wait for associated [Arbiter] to complete all commands up until it is stopped.
    ///
    /// For [Arbiter]s that have already stopped, the future will resolve immediately.
    ///
    /// # Panics
    /// Panics if called on the system Arbiter. In this situation the Arbiter's lifetime is
    /// implicitly bound by the main thread's lifetime.
    pub async fn join(self) {
        match self.stopped_rx {
            Some(rx) => {
                rx.receive().await;
            }
            None => {
                // TODO: decide if this is correct
                panic!("cannot wait on the system Arbiter's completion")
            }
        }
    }
}

/// An Arbiter represents a thread that provides an asynchronous execution environment for futures
/// and functions.
///
/// When an arbiter is created, it spawns a new [OS thread](thread), and hosts an event loop.
#[derive(Debug)]
pub struct Arbiter {
    id: usize,
    stopped_tx: OneshotBroadcastSender<()>,
    cmd_tx: mpsc::UnboundedSender<ArbiterCommand>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl Arbiter {
    /// Spawn a new Arbiter thread and start its event loop.
    ///
    /// # Panics
    /// Panics if a [System] is not registered on the current thread.
    #[allow(clippy::new_without_default)]
    pub fn new() -> ArbiterHandle {
        Self::with_tokio_rt(|| {
            default_tokio_runtime().expect("Cannot create new Arbiter's Runtime.")
        })
    }

    /// Spawn a new Arbiter using the [Tokio Runtime](tokio-runtime) returned from a closure.
    ///
    /// [tokio-runtime]: tokio::runtime::Runtime
    #[doc(hidden)]
    pub fn with_tokio_rt<F>(runtime_factory: F) -> ArbiterHandle
    where
        F: Fn() -> tokio::runtime::Runtime + Send + 'static,
    {
        eprintln!("get sys current");

        let sys = System::current();
        eprintln!("get sys id");
        let system_id = sys.id();
        eprintln!("calc arb id");
        let arb_id = COUNT.fetch_add(1, Ordering::Relaxed);

        let name = format!("actix-rt|system:{}|arbiter:{}", system_id, arb_id);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        // let ready_barrier = Arc::new(Barrier::new(2));

        let (stopped_tx, stopped_rx) = oneshot_broadcast_channel::<()>();

        eprintln!("make arb handle");
        let hnd = ArbiterHandle::new(cmd_tx.clone(), stopped_rx);

        eprintln!("make thread");
        let thread_handle = thread::Builder::new()
            .name(name.clone())
            .spawn({
                let hnd = hnd.clone();
                // let ready_barrier = Arc::clone(&ready_barrier);

                move || {
                    eprintln!("thread: make rt");
                    let rt = Runtime::from(runtime_factory());

                    eprintln!("thread: set sys");
                    System::set_current(sys);

                    // // wait until register message is sent
                    // eprintln!("thread: wait for arb registered");
                    // ready_barrier.wait();

                    eprintln!("thread: set arb handle");
                    HANDLE.with(|cell| *cell.borrow_mut() = Some(hnd.clone()));

                    // run arbiter event processing loop
                    eprintln!("thread: block on arbiter loop");
                    rt.block_on(ArbiterLoop { cmd_rx });

                    // deregister arbiter
                    eprintln!("thread: send deregister arbiter message");
                    System::current()
                        .tx()
                        .send(SystemCommand::DeregisterArbiter(arb_id))
                        .unwrap();
                }
            })
            .unwrap_or_else(|err| {
                panic!("Cannot spawn Arbiter's thread: {:?}. {:?}", &name, err)
            });

        let arb = Arbiter {
            id: arb_id,
            cmd_tx,
            stopped_tx,
            thread_handle: Some(thread_handle),
        };

        // register arbiter
        eprintln!("send register arbiter message");
        System::current()
            .tx()
            .send(SystemCommand::RegisterArbiter(arb))
            .unwrap();

        // eprintln!("inform arbiter that it is registered");
        // ready_barrier.wait();

        eprintln!("arbiter::new done");
        hnd
    }

    /// Sets up an Arbiter runner in a new System using the provided runtime local task set.
    pub(crate) fn for_system(local: &LocalSet) -> ArbiterHandle {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let hnd = ArbiterHandle::for_system(cmd_tx);

        HANDLE.with(|cell| *cell.borrow_mut() = Some(hnd.clone()));

        local.spawn_local(ArbiterLoop { cmd_rx });

        hnd
    }

    /// Return `Arbiter`'s numeric ID.
    pub(crate) fn id(&self) -> usize {
        self.id
    }

    // /// Return a handle to the this Arbiter's message sender.
    // pub fn handle(&self) -> ArbiterHandle {
    //     ArbiterHandle::new(self.cmd_tx.clone())
    // }

    /// Return a handle to the current thread's Arbiter's message sender.
    ///
    /// # Panics
    /// Panics if no Arbiter is running on the current thread.
    pub fn current() -> ArbiterHandle {
        HANDLE.with(|cell| match *cell.borrow() {
            Some(ref hnd) => hnd.clone(),
            None => panic!("Arbiter is not running."),
        })
    }

    /// Stop Arbiter from continuing it's event loop.
    ///
    /// Returns true if stop message was sent successfully and false if the Arbiter has been dropped.
    pub(crate) fn stop(&self) -> bool {
        self.cmd_tx.send(ArbiterCommand::Stop).is_ok()
    }

    // /// Send a future to the Arbiter's thread and spawn it.
    // ///
    // /// If you require a result, include a response channel in the future.
    // ///
    // /// Returns true if future was sent successfully and false if the Arbiter has died.
    // pub fn spawn<Fut>(&self, future: Fut) -> bool
    // where
    //     Fut: Future<Output = ()> + Send + 'static,
    // {
    //     self.cmd_tx
    //         .send(ArbiterCommand::Execute(Box::pin(future)))
    //         .is_ok()
    // }

    // /// Send a function to the Arbiter's thread and execute it.
    // ///
    // /// Any result from the function is discarded. If you require a result, include a response
    // /// channel in the function.
    // ///
    // /// Returns true if function was sent successfully and false if the Arbiter has died.
    // pub fn spawn_fn<F>(&self, f: F) -> bool
    // where
    //     F: FnOnce() + Send + 'static,
    // {
    //     self.spawn(async { f() })
    // }
}

impl Drop for Arbiter {
    fn drop(&mut self) {
        eprintln!("Arb::drop: joining arbiter thread");
        match self.thread_handle.take().unwrap().join() {
            Ok(()) => {}
            Err(err) => {
                eprintln!("arbiter {} thread panicked: {:?}", self.id(), err)
            }
        }
        eprintln!("Arb::drop: sending stopped tx");

        // could fail if all handles are dropped already so ignore result
        let _ = self.stopped_tx.send(());

        eprintln!("Arb::drop: done");
    }
}

/// A persistent future that processes [Arbiter] commands.
struct ArbiterLoop {
    cmd_rx: mpsc::UnboundedReceiver<ArbiterCommand>,
}

impl Future for ArbiterLoop {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // process all items currently buffered in channel
        loop {
            match ready!(Pin::new(&mut self.cmd_rx).poll_recv(cx)) {
                // channel closed; no more messages can be received
                None => return Poll::Ready(()),

                // process arbiter command
                Some(item) => match item {
                    ArbiterCommand::Stop => {
                        return Poll::Ready(());
                    }
                    ArbiterCommand::Execute(task_fut) => {
                        tokio::task::spawn_local(task_fut);
                    }
                },
            }
        }
    }
}
