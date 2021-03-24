// An example on how to build a multi-thread tokio runtime for Actix System.
// Then spawn async task that can make use of work stealing of tokio runtime.

use actix_rt::System;

fn main() {
    System::with_tokio_rt(|| {
        // build system with a multi-thread tokio runtime.
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    })
    .block_on(async_main());
}

// async main function that act similar to #[actix_web::main] or #[tokio::main]
async fn async_main() {
    let (tx, rx) = tokio::sync::oneshot::channel();

    // get a handler of system arbiter and spawn async task on it.
    System::current().arbiter().spawn(async {
        // use another tokio::spawn to get inside the context of multi thread tokio runtime.
        let h1 = tokio::spawn(async {
            println!("thread id is {:?}", std::thread::current().id());
            std::thread::sleep(std::time::Duration::from_secs(2));
        });

        // work steal happens.
        let h2 = tokio::spawn(async {
            println!("thread id is {:?}", std::thread::current().id());
        });

        h1.await.unwrap();
        h2.await.unwrap();
        let _ = tx.send(());
    });

    rx.await.unwrap();

    let (tx, rx) = tokio::sync::oneshot::channel();
    let now = std::time::Instant::now();

    // Without additional tokio::spawn. all spawned task would run on single thread.
    System::current().arbiter().spawn(async {
        println!("thread id is {:?}", std::thread::current().id());
        std::thread::sleep(std::time::Duration::from_secs(2));
        let _ = tx.send(());
    });

    // previous spawn task has blocked the system arbiter thread
    // so this task would wait for 2 sec until it can be ran.
    System::current().arbiter().spawn(async move {
        println!("thread id is {:?}", std::thread::current().id());
        assert!(now.elapsed() > std::time::Duration::from_secs(2));
    });

    rx.await.unwrap();
}
