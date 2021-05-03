use std::{
    future::Future,
    io, mem,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use actix_rt::{net::TcpStream, time::sleep, System};
use log::{error, info};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver},
    oneshot,
};

use crate::accept::{AcceptLoop, Acceptable, AcceptorStop};
use crate::server::{Server, ServerCommand};
use crate::service::{InternalServiceFactory, ServiceFactory, StreamNewService};
use crate::signals::{Signal, Signals};
use crate::socket::{
    FromConnection, MioListener, MioTcpListener, MioTcpSocket, StdSocketAddr, StdTcpListener,
    ToSocketAddrs,
};
use crate::waker_queue::WakerInterest;
use crate::worker::{ServerWorkerConfig, Worker, WorkerHandleAccept};

/// Server builder
pub struct ServerBuilder<A: Acceptable = MioListener> {
    threads: usize,
    token: usize,
    backlog: u32,
    services: Vec<Box<dyn InternalServiceFactory<A::Connection>>>,
    sockets: Vec<(usize, String, A)>,
    accept: AcceptLoop<A>,
    exit: bool,
    no_signals: bool,
    cmd: UnboundedReceiver<ServerCommand>,
    server: Server,
    notify: Vec<oneshot::Sender<()>>,
    worker_config: ServerWorkerConfig,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl<A> ServerBuilder<A>
where
    A: Acceptable + Send + Unpin + 'static,
{
    /// Create new Server builder instance
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        let server = Server::new(tx);

        Self {
            threads: num_cpus::get(),
            token: 0,
            services: Vec::new(),
            sockets: Vec::new(),
            accept: AcceptLoop::new(server.clone()),
            backlog: 2048,
            exit: false,
            no_signals: false,
            cmd: rx,
            notify: Vec::new(),
            server,
            worker_config: ServerWorkerConfig::default(),
        }
    }

    /// Set number of workers to start.
    ///
    /// By default server uses number of available logical cpu as workers
    /// count. Workers must be greater than 0.
    pub fn workers(mut self, num: usize) -> Self {
        assert_ne!(num, 0, "workers must be greater than 0");
        self.threads = num;
        self
    }

    /// Set max number of threads for each worker's blocking task thread pool.
    ///
    /// One thread pool is set up **per worker**; not shared across workers.
    ///
    /// # Examples:
    /// ```
    /// # use actix_server::ServerBuilder;
    /// let builder = ServerBuilder::default()
    ///     .workers(4) // server has 4 worker thread.
    ///     .worker_max_blocking_threads(4); // every worker has 4 max blocking threads.
    /// ```
    ///
    /// See [tokio::runtime::Builder::max_blocking_threads] for behavior reference.
    pub fn worker_max_blocking_threads(mut self, num: usize) -> Self {
        self.worker_config.max_blocking_threads(num);
        self
    }

    /// Set the maximum number of pending connections.
    ///
    /// This refers to the number of clients that can be waiting to be served.
    /// Exceeding this number results in the client getting an error when
    /// attempting to connect. It should only affect servers under significant
    /// load.
    ///
    /// Generally set in the 64-2048 range. Default value is 2048.
    ///
    /// This method should be called before `bind()` method call.
    pub fn backlog(mut self, num: u32) -> Self {
        self.backlog = num;
        self
    }

    /// Sets the maximum per-worker number of concurrent connections.
    ///
    /// All socket listeners will stop accepting connections when this limit is
    /// reached for each worker.
    ///
    /// By default max connections is set to a 25k per worker.
    pub fn maxconn(mut self, num: usize) -> Self {
        self.worker_config.max_concurrent_connections(num);
        self
    }

    /// Stop Actix system.
    pub fn system_exit(mut self) -> Self {
        self.exit = true;
        self
    }

    /// Disable signal handling.
    pub fn disable_signals(mut self) -> Self {
        self.no_signals = true;
        self
    }

    /// Timeout for graceful workers shutdown in seconds.
    ///
    /// After receiving a stop signal, workers have this much time to finish serving requests.
    /// Workers still alive after the timeout are force dropped.
    ///
    /// By default shutdown timeout sets to 30 seconds.
    pub fn shutdown_timeout(mut self, sec: u64) -> Self {
        self.worker_config
            .shutdown_timeout(Duration::from_secs(sec));
        self
    }

    fn next_token(&mut self) -> usize {
        let token = self.token;
        self.token += 1;
        token
    }

    fn start_worker(&self, idx: usize) -> WorkerHandleAccept<A::Connection> {
        let services = self.services.iter().map(|v| v.clone_factory()).collect();
        let config = self.worker_config;
        let waker_queue = self.accept.waker_owned();
        Worker::start(idx, services, waker_queue, config)
    }

    fn handle_signal(&mut self, sig: Signal) {
        // Signals support
        // Handle `SIGINT`, `SIGTERM`, `SIGQUIT` signals and stop actix system
        match sig {
            Signal::Int => {
                info!("SIGINT received, exiting");
                self.exit = true;
                self.handle_cmd(ServerCommand::Stop {
                    graceful: false,
                    completion: None,
                })
            }
            Signal::Term => {
                info!("SIGTERM received, stopping");
                self.exit = true;
                self.handle_cmd(ServerCommand::Stop {
                    graceful: true,
                    completion: None,
                })
            }
            Signal::Quit => {
                info!("SIGQUIT received, exiting");
                self.exit = true;
                self.handle_cmd(ServerCommand::Stop {
                    graceful: false,
                    completion: None,
                })
            }
            _ => (),
        }
    }

    fn handle_cmd(&mut self, item: ServerCommand) {
        match item {
            ServerCommand::Pause(tx) => {
                self.accept.wake(WakerInterest::Pause);
                let _ = tx.send(());
            }
            ServerCommand::Resume(tx) => {
                self.accept.wake(WakerInterest::Resume);
                let _ = tx.send(());
            }
            ServerCommand::Signal(sig) => self.handle_signal(sig),
            ServerCommand::Notify(tx) => {
                self.notify.push(tx);
            }
            ServerCommand::Stop {
                graceful,
                completion,
            } => {
                let exit = self.exit;

                // stop accept thread
                let (stop, rx) = AcceptorStop::new(graceful);

                self.accept.wake(WakerInterest::Stop(stop));
                let notify = std::mem::take(&mut self.notify);

                actix_rt::spawn(async move {
                    for rx in rx.await.unwrap_or_else(|_| Vec::new()) {
                        let _ = rx.await;
                    }

                    if let Some(tx) = completion {
                        let _ = tx.send(());
                    }
                    for tx in notify {
                        let _ = tx.send(());
                    }

                    if exit {
                        sleep(Duration::from_millis(300)).await;
                        System::current().stop();
                    }
                });
            }
            ServerCommand::WorkerFaulted(idx) => {
                error!("Worker has died {:?}, restarting", idx);

                let handle = self.start_worker(idx);
                self.accept.wake(WakerInterest::Worker(handle));
            }
        }
    }

    /// Starts processing incoming connections and return server controller.
    pub fn run(mut self) -> Server {
        if self.sockets.is_empty() {
            panic!("Server should have at least one bound socket");
        } else {
            info!("Starting {} workers", self.threads);

            // start workers
            let handles = (0..self.threads)
                .map(|idx| self.start_worker(idx))
                .collect();

            // start accept thread
            for sock in &self.sockets {
                info!("Starting \"{}\" service on {:?}", sock.1, sock.2);
            }
            self.accept.start(
                mem::take(&mut self.sockets)
                    .into_iter()
                    .map(|t| (t.0, t.2))
                    .collect(),
                handles,
            );

            // handle signals
            if !self.no_signals {
                Signals::start(self.server.clone());
            }

            // start http server actor
            let server = self.server.clone();
            actix_rt::spawn(self);
            server
        }
    }

    #[doc(hidden)]
    pub fn bind_acceptable<F, Io>(
        mut self,
        name: &str,
        addr: StdSocketAddr,
        lst: A,
        factory: F,
    ) -> Self
    where
        F: ServiceFactory<Io, A::Connection>,
        Io: FromConnection<A::Connection> + Send + 'static,
    {
        let token = self.next_token();
        self.services.push(StreamNewService::create(
            name.to_string(),
            token,
            factory,
            addr,
        ));

        self.sockets.push((token, name.to_string(), lst));

        self
    }
}

impl ServerBuilder {
    /// Add new service to the server.
    pub fn bind<F, U, N>(mut self, name: N, addr: U, factory: F) -> io::Result<Self>
    where
        F: ServiceFactory<TcpStream>,
        N: AsRef<str>,
        U: ToSocketAddrs,
    {
        let sockets = bind_addr(addr, self.backlog)?;

        for lst in sockets {
            let addr = lst.local_addr()?;
            let lst = MioListener::Tcp(lst);

            self = self.bind_acceptable(name.as_ref(), addr, lst, factory.clone());
        }

        Ok(self)
    }

    /// Add new service to the server.
    pub fn listen<F, N: AsRef<str>>(
        self,
        name: N,
        lst: StdTcpListener,
        factory: F,
    ) -> io::Result<Self>
    where
        F: ServiceFactory<TcpStream>,
    {
        lst.set_nonblocking(true)?;

        let addr = lst.local_addr()?;
        let lst = MioListener::from(lst);

        Ok(self.bind_acceptable(name.as_ref(), addr, lst, factory))
    }
}

#[cfg(unix)]
impl ServerBuilder {
    /// Add new unix domain service to the server.
    pub fn bind_uds<F, U, N>(self, name: N, addr: U, factory: F) -> io::Result<Self>
    where
        F: ServiceFactory<actix_rt::net::UnixStream>,
        N: AsRef<str>,
        U: AsRef<std::path::Path>,
    {
        // The path must not exist when we try to bind.
        // Try to remove it to avoid bind error.
        if let Err(e) = std::fs::remove_file(addr.as_ref()) {
            // NotFound is expected and not an issue. Anything else is.
            if e.kind() != io::ErrorKind::NotFound {
                return Err(e);
            }
        }

        let lst = crate::socket::StdUnixListener::bind(addr)?;
        self.listen_uds(name, lst, factory)
    }

    /// Add new unix domain service to the server.
    /// Useful when running as a systemd service and
    /// a socket FD can be acquired using the systemd crate.
    pub fn listen_uds<F, N>(
        self,
        name: N,
        lst: crate::socket::StdUnixListener,
        factory: F,
    ) -> io::Result<Self>
    where
        F: ServiceFactory<actix_rt::net::UnixStream>,
        N: AsRef<str>,
    {
        lst.set_nonblocking(true)?;

        let addr = "127.0.0.1:8080".parse().unwrap();

        let lst = MioListener::from(lst);

        Ok(self.bind_acceptable(name.as_ref(), addr, lst, factory))
    }
}

impl<A: Acceptable> Future for ServerBuilder<A>
where
    A: Acceptable + Send + Unpin + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.cmd).poll_recv(cx) {
                Poll::Ready(Some(it)) => this.handle_cmd(it),
                _ => return Poll::Pending,
            }
        }
    }
}

pub(super) fn bind_addr<S: ToSocketAddrs>(
    addr: S,
    backlog: u32,
) -> io::Result<Vec<MioTcpListener>> {
    let mut err = None;
    let mut succ = false;
    let mut sockets = Vec::new();
    for addr in addr.to_socket_addrs()? {
        match create_tcp_listener(addr, backlog) {
            Ok(lst) => {
                succ = true;
                sockets.push(lst);
            }
            Err(e) => err = Some(e),
        }
    }

    if !succ {
        if let Some(e) = err.take() {
            Err(e)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Can not bind to address.",
            ))
        }
    } else {
        Ok(sockets)
    }
}

fn create_tcp_listener(addr: StdSocketAddr, backlog: u32) -> io::Result<MioTcpListener> {
    let socket = match addr {
        StdSocketAddr::V4(_) => MioTcpSocket::new_v4()?,
        StdSocketAddr::V6(_) => MioTcpSocket::new_v6()?,
    };

    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(backlog)
}
