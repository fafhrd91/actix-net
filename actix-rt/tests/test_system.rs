use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use actix_rt::{time, Arbiter, System};
use tokio::sync::oneshot;

#[test]
#[should_panic]
fn no_system_current_panic() {
    System::current();
}

#[test]
fn try_current_no_system() {
    assert!(System::try_current().is_none())
}

#[test]
fn try_current_with_system() {
    System::new().block_on(async { assert!(System::try_current().is_some()) });
}

#[test]
fn non_static_block_on() {
    let string = String::from("test_str");
    let string = string.as_str();

    let sys = System::new();

    sys.block_on(async {
        actix_rt::time::sleep(Duration::from_millis(1)).await;
        assert_eq!("test_str", string);
    });

    let rt = actix_rt::Runtime::new().unwrap();

    rt.block_on(async {
        actix_rt::time::sleep(Duration::from_millis(1)).await;
        assert_eq!("test_str", string);
    });

    System::current().stop();
    sys.run().unwrap();
}

#[test]
fn await_for_timer() {
    let time = Duration::from_secs(1);
    let instant = Instant::now();

    System::new().block_on(async move {
        time::sleep(time).await;
    });

    assert!(
        instant.elapsed() >= time,
        "Calling `block_on` should poll awaited future to completion."
    );
}

#[test]
fn system_arbiter_spawn() {
    let runner = System::new();

    let (tx, rx) = oneshot::channel();
    let sys = System::current();

    thread::spawn(|| {
        // this thread will have no arbiter in it's thread local so call will panic
        Arbiter::current();
    })
    .join()
    .unwrap_err();

    let thread = thread::spawn(|| {
        // this thread will have no arbiter in it's thread local so use the system handle instead
        System::set_current(sys);
        let sys = System::current();

        let arb = sys.arbiter();
        arb.spawn(async move {
            tx.send(42u32).unwrap();
            System::current().stop();
        });
    });

    assert_eq!(runner.block_on(rx).unwrap(), 42);
    thread.join().unwrap();
}

struct Atom(Arc<AtomicBool>);

impl Drop for Atom {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn system_stop_arbiter_join_barrier() {
    let sys = System::new();
    let arb = Arbiter::new();

    let atom = Atom(Arc::new(AtomicBool::new(false)));

    // arbiter should be alive to receive spawn msg
    assert!(Arbiter::current().spawn_fn(|| {}));
    assert!(arb.spawn_fn(move || {
        // thread should get dropped during sleep
        thread::sleep(Duration::from_secs(2));

        // pointless load to move atom into scope
        atom.0.load(Ordering::SeqCst);

        panic!("spawned fn (thread) should be dropped during sleep");
    }));

    System::current().stop();
    sys.run().unwrap();

    // arbiter should be dead and return false
    assert!(!Arbiter::current().spawn_fn(|| {}));
    assert!(!arb.spawn_fn(|| {}));
}
