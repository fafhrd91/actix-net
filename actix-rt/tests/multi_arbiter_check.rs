//! Derived from this comment:
//! https://github.com/actix/actix/issues/464#issuecomment-779427825

use std::{thread, time::Duration};

use actix_rt::{Arbiter, System};
use tokio::sync::mpsc;

#[test]
fn actix_sample() {
    let sys = System::new();
    let arb = Arbiter::new();

    let (_tx, mut rx) = mpsc::unbounded_channel::<()>();

    // create "actor"
    arb.spawn_fn(move || {
        let a = A;

        actix_rt::spawn(async move {
            while let Some(_) = rx.recv().await {
                println!("{:?}", a);
            }
        });
    });

    System::current().stop();

    // all arbiters must be dropped when sys.run returns
    sys.run().unwrap();

    thread::sleep(Duration::from_millis(100));
}

#[derive(Debug)]
struct A;

impl Drop for A {
    fn drop(&mut self) {
        println!("start drop");
        thread::sleep(Duration::from_millis(200));
        println!("finish drop");
    }
}
