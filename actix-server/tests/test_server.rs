use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::{net, thread, time::Duration};

use actix_rt::{net::TcpStream, time::sleep};
use actix_server::Server;
use actix_service::fn_service;
use actix_utils::future::ok;
use futures_util::future::lazy;

fn unused_addr() -> net::SocketAddr {
    let addr: net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let socket = mio::net::TcpSocket::new_v4().unwrap();
    socket.bind(addr).unwrap();
    socket.set_reuseaddr(true).unwrap();
    let tcp = socket.listen(32).unwrap();
    tcp.local_addr().unwrap()
}

#[test]
fn test_bind() {
    let addr = unused_addr();
    let (tx, rx) = mpsc::channel();

    let h = thread::spawn(move || {
        actix_rt::System::new().block_on(async {
            let server = Server::build()
                .workers(1)
                .disable_signals()
                .bind("test", addr, move || fn_service(|_| ok::<_, ()>(())))
                .unwrap()
                .run();
            tx.send(server.handle()).unwrap();
            server.await
        })
    });
    let handle = rx.recv().unwrap();

    thread::sleep(Duration::from_millis(500));
    assert!(net::TcpStream::connect(addr).is_ok());
    let _ = handle.stop(true);
    let _ = h.join().unwrap();
}

#[test]
fn test_listen() {
    let addr = unused_addr();
    let (tx, rx) = mpsc::channel();

    let h = thread::spawn(move || {
        let lst = net::TcpListener::bind(addr).unwrap();

        actix_rt::System::new().block_on(async {
            let server = Server::build()
                .disable_signals()
                .workers(1)
                .listen("test", lst, move || {
                    fn_service(|_| async { Ok::<_, ()>(()) })
                })
                .unwrap()
                .run();

            let _ = tx.send(server.handle());

            server.await
        })
    });

    let handle = rx.recv().unwrap();

    thread::sleep(Duration::from_millis(500));
    assert!(net::TcpStream::connect(addr).is_ok());
    let _ = handle.stop(true);
    let _ = h.join().unwrap();
}

#[test]
#[cfg(unix)]
fn test_start() {
    use std::io::Read;

    use actix_codec::{BytesCodec, Framed};
    use bytes::Bytes;
    use futures_util::sink::SinkExt;

    let addr = unused_addr();
    let (tx, rx) = mpsc::channel();

    let h = thread::spawn(move || {
        actix_rt::System::new().block_on(async {
            let server = Server::build()
                .backlog(100)
                .disable_signals()
                .bind("test", addr, move || {
                    fn_service(|io: TcpStream| async move {
                        let mut f = Framed::new(io, BytesCodec);
                        f.send(Bytes::from_static(b"test")).await.unwrap();
                        Ok::<_, ()>(())
                    })
                })
                .unwrap()
                .run();

            let _ = tx.send((server.handle(), actix_rt::System::current()));
            let _ = server.await;
        });
    });

    let (srv, sys) = rx.recv().unwrap();

    let mut buf = [1u8; 4];
    let mut conn = net::TcpStream::connect(addr).unwrap();
    let _ = conn.read_exact(&mut buf);
    assert_eq!(buf, b"test"[..]);

    // pause
    let _ = srv.pause();
    thread::sleep(Duration::from_millis(200));
    let mut conn = net::TcpStream::connect(addr).unwrap();
    conn.set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let res = conn.read_exact(&mut buf);
    assert!(res.is_err());

    // resume
    let _ = srv.resume();
    thread::sleep(Duration::from_millis(100));
    assert!(net::TcpStream::connect(addr).is_ok());
    assert!(net::TcpStream::connect(addr).is_ok());
    assert!(net::TcpStream::connect(addr).is_ok());

    let mut buf = [0u8; 4];
    let mut conn = net::TcpStream::connect(addr).unwrap();
    let _ = conn.read_exact(&mut buf);
    assert_eq!(buf, b"test"[..]);

    // stop
    let _ = srv.stop(false);
    thread::sleep(Duration::from_millis(100));
    assert!(net::TcpStream::connect(addr).is_err());

    thread::sleep(Duration::from_millis(100));
    sys.stop();
    let _ = h.join();
}

#[test]
fn test_configure() {
    let addr1 = unused_addr();
    let addr2 = unused_addr();
    let addr3 = unused_addr();
    let (tx, rx) = mpsc::channel();
    let num = Arc::new(AtomicUsize::new(0));
    let num2 = num.clone();

    let h = thread::spawn(move || {
        let num = num2.clone();
        actix_rt::System::new().block_on(async {
            let server = Server::build()
                .disable_signals()
                .configure(move |cfg| {
                    let num = num.clone();
                    let lst = net::TcpListener::bind(addr3).unwrap();
                    cfg.bind("addr1", addr1)
                        .unwrap()
                        .bind("addr2", addr2)
                        .unwrap()
                        .listen("addr3", lst)
                        .apply(move |rt| {
                            let num = num.clone();
                            rt.service("addr1", fn_service(|_| ok::<_, ()>(())));
                            rt.service("addr3", fn_service(|_| ok::<_, ()>(())));
                            rt.on_start(lazy(move |_| {
                                let _ = num.fetch_add(1, Ordering::Relaxed);
                            }))
                        })
                })
                .unwrap()
                .workers(1)
                .run();

            let _ = tx.send((server.handle(), actix_rt::System::current()));
            let _ = server.await;
        });
    });

    let (server, sys) = rx.recv().unwrap();
    thread::sleep(Duration::from_millis(500));

    assert!(net::TcpStream::connect(addr1).is_ok());
    assert!(net::TcpStream::connect(addr2).is_ok());
    assert!(net::TcpStream::connect(addr3).is_ok());
    assert_eq!(num.load(Ordering::Relaxed), 1);
    let _ = server.stop(true);
    sys.stop();
    let _ = h.join();
}

#[actix_rt::test]
async fn test_max_concurrent_connections() {
    // Note:
    // A tcp listener would accept connects based on it's backlog setting.
    //
    // The limit test on the other hand is only for concurrent tcp stream limiting a work
    // thread accept.

    use tokio::io::AsyncWriteExt;

    let addr = unused_addr();
    let (tx, rx) = mpsc::channel();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    let max_conn = 3;

    let h = thread::spawn(move || {
        actix_rt::System::new().block_on(async {
            let server = Server::build()
                // Set a relative higher backlog.
                .backlog(12)
                // max connection for a worker is 3.
                .maxconn(max_conn)
                .workers(1)
                .disable_signals()
                .bind("test", addr, move || {
                    let counter = counter.clone();
                    fn_service(move |_io: TcpStream| {
                        let counter = counter.clone();
                        async move {
                            counter.fetch_add(1, Ordering::SeqCst);
                            sleep(Duration::from_secs(20)).await;
                            counter.fetch_sub(1, Ordering::SeqCst);
                            Ok::<(), ()>(())
                        }
                    })
                })?
                .run();

            let _ = tx.send((server.handle(), actix_rt::System::current()));

            server.await
        })
    });

    let (srv, sys) = rx.recv().unwrap();

    let mut conns = vec![];

    for _ in 0..12 {
        let conn = tokio::net::TcpStream::connect(addr).await.unwrap();
        conns.push(conn);
    }

    sleep(Duration::from_secs(5)).await;

    // counter would remain at 3 even with 12 successful connection.
    // and 9 of them remain in backlog.
    assert_eq!(max_conn, counter_clone.load(Ordering::SeqCst));

    for mut conn in conns {
        conn.shutdown().await.unwrap();
    }

    srv.stop(false).await;

    sys.stop();
    let _ = h.join().unwrap();
}

#[actix_rt::test]
async fn test_service_restart() {
    use std::task::{Context, Poll};

    use actix_service::{fn_factory, Service};
    use futures_core::future::LocalBoxFuture;
    use tokio::io::AsyncWriteExt;

    struct TestService(Arc<AtomicUsize>);

    impl Service<TcpStream> for TestService {
        type Response = ();
        type Error = ();
        type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let TestService(ref counter) = self;
            let c = counter.fetch_add(1, Ordering::SeqCst);
            // Force the service to restart on first readiness check.
            if c > 0 {
                Poll::Ready(Ok(()))
            } else {
                Poll::Ready(Err(()))
            }
        }

        fn call(&self, _: TcpStream) -> Self::Future {
            Box::pin(async { Ok(()) })
        }
    }

    let addr1 = unused_addr();
    let addr2 = unused_addr();
    let (tx, rx) = mpsc::channel();
    let num = Arc::new(AtomicUsize::new(0));
    let num2 = Arc::new(AtomicUsize::new(0));

    let num_clone = num.clone();
    let num2_clone = num2.clone();

    let h = thread::spawn(move || {
        actix_rt::System::new().block_on(async {
            let server = Server::build()
                .backlog(1)
                .disable_signals()
                .configure(move |cfg| {
                    let num = num.clone();
                    let num2 = num2.clone();
                    cfg.bind("addr1", addr1)
                        .unwrap()
                        .bind("addr2", addr2)
                        .unwrap()
                        .apply(move |rt| {
                            let num = num.clone();
                            let num2 = num2.clone();
                            rt.service(
                                "addr1",
                                fn_factory(move || {
                                    let num = num.clone();
                                    async move { Ok::<_, ()>(TestService(num)) }
                                }),
                            );
                            rt.service(
                                "addr2",
                                fn_factory(move || {
                                    let num2 = num2.clone();
                                    async move { Ok::<_, ()>(TestService(num2)) }
                                }),
                            );
                        })
                })
                .unwrap()
                .workers(1)
                .run();

            let _ = tx.send((server.handle(), actix_rt::System::current()));
            server.await
        })
    });

    let (server, sys) = rx.recv().unwrap();

    for _ in 0..5 {
        TcpStream::connect(addr1)
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        TcpStream::connect(addr2)
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
    }

    sleep(Duration::from_secs(3)).await;

    assert!(num_clone.load(Ordering::SeqCst) > 5);
    assert!(num2_clone.load(Ordering::SeqCst) > 5);

    sys.stop();
    let _ = server.stop(false);
    let _ = h.join().unwrap();

    let addr1 = unused_addr();
    let addr2 = unused_addr();
    let (tx, rx) = mpsc::channel();
    let num = Arc::new(AtomicUsize::new(0));
    let num2 = Arc::new(AtomicUsize::new(0));

    let num_clone = num.clone();
    let num2_clone = num2.clone();

    let h = thread::spawn(move || {
        let num = num.clone();
        actix_rt::System::new().block_on(async {
            let server = Server::build()
                .backlog(1)
                .disable_signals()
                .bind("addr1", addr1, move || {
                    let num = num.clone();
                    fn_factory(move || {
                        let num = num.clone();
                        async move { Ok::<_, ()>(TestService(num)) }
                    })
                })
                .unwrap()
                .bind("addr2", addr2, move || {
                    let num2 = num2.clone();
                    fn_factory(move || {
                        let num2 = num2.clone();
                        async move { Ok::<_, ()>(TestService(num2)) }
                    })
                })
                .unwrap()
                .workers(1)
                .run();

            let _ = tx.send((server.handle(), actix_rt::System::current()));
            server.await
        })
    });

    let (server, sys) = rx.recv().unwrap();

    for _ in 0..5 {
        TcpStream::connect(addr1)
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        TcpStream::connect(addr2)
            .await
            .unwrap()
            .shutdown()
            .await
            .unwrap();
    }

    sleep(Duration::from_secs(3)).await;

    assert!(num_clone.load(Ordering::SeqCst) > 5);
    assert!(num2_clone.load(Ordering::SeqCst) > 5);

    sys.stop();
    let _ = server.stop(false);
    let _ = h.join().unwrap();
}

#[ignore]
#[actix_rt::test]
async fn worker_restart() {
    use actix_service::{Service, ServiceFactory};
    use futures_core::future::LocalBoxFuture;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct TestServiceFactory(Arc<AtomicUsize>);

    impl ServiceFactory<TcpStream> for TestServiceFactory {
        type Response = ();
        type Error = ();
        type Config = ();
        type Service = TestService;
        type InitError = ();
        type Future = LocalBoxFuture<'static, Result<Self::Service, Self::InitError>>;

        fn new_service(&self, _: Self::Config) -> Self::Future {
            let counter = self.0.fetch_add(1, Ordering::Relaxed);

            Box::pin(async move { Ok(TestService(counter)) })
        }
    }

    struct TestService(usize);

    impl Service<TcpStream> for TestService {
        type Response = ();
        type Error = ();
        type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

        actix_service::always_ready!();

        fn call(&self, stream: TcpStream) -> Self::Future {
            let counter = self.0;

            let mut stream = stream.into_std().unwrap();
            use std::io::Write;
            let str = counter.to_string();
            let buf = str.as_bytes();

            let mut written = 0;

            while written < buf.len() {
                if let Ok(n) = stream.write(&buf[written..]) {
                    written += n;
                }
            }
            stream.flush().unwrap();
            stream.shutdown(net::Shutdown::Write).unwrap();

            // force worker 2 to restart service once.
            if counter == 2 {
                panic!("panic on purpose")
            } else {
                Box::pin(async { Ok(()) })
            }
        }
    }

    let addr = unused_addr();
    let (tx, rx) = mpsc::channel();

    let counter = Arc::new(AtomicUsize::new(1));
    let h = thread::spawn(move || {
        let counter = counter.clone();
        actix_rt::System::new().block_on(async {
            let server = Server::build()
                .disable_signals()
                .bind("addr", addr, move || TestServiceFactory(counter.clone()))
                .unwrap()
                .workers(2)
                .run();

            let _ = tx.send((server.handle(), actix_rt::System::current()));
            server.await
        })
    });

    let (server, sys) = rx.recv().unwrap();

    sleep(Duration::from_secs(3)).await;

    let mut buf = [0; 8];

    // worker 1 would not restart and return it's id consistently.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    let id = String::from_utf8_lossy(&buf[0..n]);
    assert_eq!("1", id);
    stream.shutdown().await.unwrap();

    // worker 2 dead after return response.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    let id = String::from_utf8_lossy(&buf[0..n]);
    assert_eq!("2", id);
    stream.shutdown().await.unwrap();

    // request to worker 1
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    let id = String::from_utf8_lossy(&buf[0..n]);
    assert_eq!("1", id);
    stream.shutdown().await.unwrap();

    // TODO: Remove sleep if it can pass CI.
    sleep(Duration::from_secs(3)).await;

    // worker 2 restarting and work goes to worker 1.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    let id = String::from_utf8_lossy(&buf[0..n]);
    assert_eq!("1", id);
    stream.shutdown().await.unwrap();

    // TODO: Remove sleep if it can pass CI.
    sleep(Duration::from_secs(3)).await;

    // worker 2 restarted but worker 1 was still the next to accept connection.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    let id = String::from_utf8_lossy(&buf[0..n]);
    assert_eq!("1", id);
    stream.shutdown().await.unwrap();

    // TODO: Remove sleep if it can pass CI.
    sleep(Duration::from_secs(3)).await;

    // worker 2 accept connection again but it's id is 3.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    let id = String::from_utf8_lossy(&buf[0..n]);
    assert_eq!("3", id);
    stream.shutdown().await.unwrap();

    sys.stop();
    let _ = server.stop(false);
    let _ = h.join().unwrap();
}
