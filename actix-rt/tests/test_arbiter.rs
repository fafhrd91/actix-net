use std::{
    sync::mpsc::channel as std_channel,
    time::{Duration, Instant},
};

use actix_rt::{time, Arbiter, System};

#[test]
#[should_panic]
fn no_system_arbiter_new_panic() {
    Arbiter::new();
}

#[test]
fn join_arbiter_wait_fut() {
    let time = Duration::from_secs(1);
    let instant = Instant::now();

    System::new().block_on(async move {
        let arbiter = Arbiter::new();

        arbiter.spawn(async move {
            time::sleep(time).await;
            Arbiter::current().stop();
        });

        arbiter.join().await;
    });

    assert!(
        instant.elapsed() >= time,
        "Join on another arbiter should complete only when it calls stop"
    );
}

#[test]
fn join_arbiter_wait_fn() {
    let time = Duration::from_secs(1);
    let instant = Instant::now();

    System::new().block_on(async move {
        let arbiter = Arbiter::new();

        arbiter.spawn_fn(move || {
            actix_rt::spawn(async move {
                time::sleep(time).await;
                Arbiter::current().stop();
            });
        });

        arbiter.join().await;
    });

    assert!(
        instant.elapsed() >= time,
        "Join on an arbiter that has used actix_rt::spawn should wait for said future"
    );
}

#[test]
fn join_arbiter_early_stop_call() {
    let time = Duration::from_secs(1);
    let instant = Instant::now();

    System::new().block_on(async move {
        let arbiter = Arbiter::new();

        arbiter.spawn(Box::pin(async move {
            time::sleep(time).await;
            Arbiter::current().stop();
        }));

        arbiter.stop();
        arbiter.join().await;
    });

    assert!(
        instant.elapsed() < time,
        "Premature stop of Arbiter should conclude regardless of it's current state."
    );
}

#[test]
fn arbiter_spawn_fn_runs() {
    let sys = System::new();

    let (tx, rx) = std_channel::<u32>();

    let arbiter = Arbiter::new();
    arbiter.spawn_fn(move || {
        tx.send(42).unwrap();
        System::current().stop();
    });

    let num = rx.recv().unwrap();
    assert_eq!(num, 42);

    sys.run().unwrap();
}

#[test]
fn arbiter_inner_panic() {
    let sys = System::new();

    let (tx, rx) = std_channel::<u32>();

    let arbiter = Arbiter::new();

    // spawned panics should not cause arbiter to crash
    arbiter.spawn(async { panic!("inner panic; will be caught") });
    arbiter.spawn_fn(|| panic!("inner panic; will be caught"));

    arbiter.spawn(async move { tx.send(42).unwrap() });

    let num = rx.recv().unwrap();
    assert_eq!(num, 42);

    System::current().stop();
    sys.run().unwrap();
}
