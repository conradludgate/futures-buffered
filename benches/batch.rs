use criterion::{criterion_group, criterion_main, Criterion};
use futures_buffered::FuturesUnorderedBounded;
use futures_util::{stream::FuturesUnordered, StreamExt};
use hyper::{
    client::conn::{self, ResponseFuture, SendRequest},
    Body, Request,
};
use tokio::net::TcpStream;

const BATCH: usize = 256;
const TOTAL: usize = 512000;

fn batch(c: &mut Criterion) {
    // setup a tokio runtime for our tests
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // create a tcp connection
    let stream = runtime
        .block_on(TcpStream::connect("example.com:80"))
        .unwrap();

    // perform the http handshakes
    let (mut rs, conn) = runtime.block_on(conn::handshake(stream)).unwrap();
    runtime.spawn(conn);

    /// make http request to example.com and read the response
    fn make_req(rs: &mut SendRequest<Body>) -> ResponseFuture {
        let req = Request::builder()
            .header("Host", "example.com")
            .method("GET")
            .body(Body::from(""))
            .unwrap();
        rs.send_request(req)
    }

    let mut queue = FuturesUnordered::new();
    c.bench_function("FuturesUnordered", |b| {
        b.iter(|| {
            for _ in 0..BATCH {
                queue.push(make_req(&mut rs))
            }
            for _ in BATCH..TOTAL {
                runtime.block_on(queue.next());
                queue.push(make_req(&mut rs))
            }
            for _ in 0..BATCH {
                runtime.block_on(queue.next());
            }
        })
    });

    let mut queue = FuturesUnorderedBounded::new(BATCH);
    c.bench_function("FuturesUnorderedBounded", |b| {
        b.iter(|| {
            for _ in 0..BATCH {
                queue.push(make_req(&mut rs)).map_err(drop).unwrap();
            }
            for _ in BATCH..TOTAL {
                runtime.block_on(queue.next());
                queue.push(make_req(&mut rs)).map_err(drop).unwrap();
            }
            for _ in 0..BATCH {
                runtime.block_on(queue.next());
            }
        })
    });

    c.bench_function("futures::join_all", |b| {
        b.iter(|| {
            let mut futs = Vec::new();
            for _ in 0..BATCH * 8 {
                futs.push(make_req(&mut rs))
            }
            runtime.block_on(futures::future::join_all(futs));
        })
    });

    c.bench_function("crate::join_all", |b| {
        b.iter(|| {
            let mut futs = Vec::new();
            for _ in 0..BATCH * 8 {
                futs.push(make_req(&mut rs))
            }
            runtime.block_on(futures_buffered::join_all(futs));
        })
    });
}

criterion_group!(benches, batch);

criterion_main!(benches);
