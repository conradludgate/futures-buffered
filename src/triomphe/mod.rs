// Copyright The Rust Project Developers + Manishearth
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Fork of triomphe. This has the following advantages over triomphe::ThinArc:
//!
//! * We can backtrack from an entry within the ThinArc, to the ThinArc itself.

mod arc;
mod arc_borrow;
mod header;
mod iterator_as_exact_size_iterator;
mod thin_arc;

pub use arc::*;
pub use arc_borrow::*;
pub use header::*;
pub use thin_arc::*;

// `no_std`-compatible abort by forcing a panic while already panicking.
#[cold]
fn abort() -> ! {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!()
        }
    }
    let _double_panicer = PanicOnDrop;
    panic!();
}
