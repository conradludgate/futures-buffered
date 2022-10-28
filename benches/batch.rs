use criterion::{criterion_group, criterion_main, Criterion};
use futures::{stream::FuturesUnordered, StreamExt};
use futures_buffered::ConcurrentProcessQueue;
use hyper::{
    client::conn::{self, ResponseFuture, SendRequest},
    Body, Request,
};
use tokio::net::TcpStream;

const BATCH: usize = 50;
const TOTAL: usize = 50000;

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

    c.bench_function("ll_based", |b| {
        b.iter(|| {
            let mut queue = FuturesUnordered::new();

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

    c.bench_function("vec_based", |b| {
        b.iter(|| {
            let mut queue = ConcurrentProcessQueue::new(BATCH);

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
}

criterion_group!(benches, batch);

criterion_main!(benches);
