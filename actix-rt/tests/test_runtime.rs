use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::channel as std_channel,
        Arc,
    },
    time::Duration,
};

use actix_rt::{Arbiter, System};

#[test]
fn wait_for_spawns() {
    let rt = actix_rt::Runtime::new().unwrap();

    let handle = rt.spawn(async {
        println!("running on the runtime");
        // assertion panic is caught at task boundary
        assert_eq!(1, 2);
    });

    assert!(rt.block_on(handle).is_err());
}

#[test]
fn new_system_with_tokio() {
    let (tx, rx) = std_channel();

    let res = System::with_tokio_rt(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .thread_keep_alive(Duration::from_millis(1000))
            .worker_threads(2)
            .max_blocking_threads(2)
            .on_thread_start(|| {})
            .on_thread_stop(|| {})
            .build()
            .unwrap()
    })
    .block_on(async {
        actix_rt::time::sleep(Duration::from_millis(1)).await;

        tokio::task::spawn(async move {
            tx.send(42).unwrap();
        })
        .await
        .unwrap();

        123usize
    });

    assert_eq!(res, 123);
    assert_eq!(rx.recv().unwrap(), 42);
}

#[test]
fn new_arbiter_with_tokio() {
    let sys = System::new();

    let arb = Arbiter::with_tokio_rt(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    });

    let counter = Arc::new(AtomicBool::new(true));

    let counter1 = counter.clone();
    let did_spawn = arb.spawn(async move {
        actix_rt::time::sleep(Duration::from_millis(1)).await;
        counter1.store(false, Ordering::SeqCst);
        Arbiter::current().stop();
        System::current().stop();
    });

    sys.run().unwrap();

    assert!(did_spawn);
    assert_eq!(false, counter.load(Ordering::SeqCst));
}
