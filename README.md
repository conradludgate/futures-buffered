# futures-buffered

This project provides a single future structure: `ConcurrentProcessQueue`.

Much like [`futures::FuturesUnordered`](https://docs.rs/futures/0.3.25/futures/stream/struct.FuturesUnordered.html), this is a thread-safe, `Pin` friendly, lifetime friendly, concurrent processing stream.

The is different to `FuturesUnordered` in that `ConcurrentProcessQueue` has a fixed capacity for processing count. This means it's less flexible, but produces better memory efficiency.

## Benchmarks

### Speed

Running 512000 http requests (over an already establish HTTP2 connection) with 256 concurrent jobs
in a single threaded tokio runtime:

```
FuturesUnordered        time:   [220.20 ms 220.97 ms 221.80 ms]
ConcurrentProcessQueue  time:   [208.73 ms 209.26 ms 209.86 ms]
```

### Memory usage

Running 512000 `Ready<i32>` futures with 256 concurrent jobs in a single threaded tokio runtime.

- count: the number of times alloc/dealloc was called
- alloc: the number of cumulative bytes allocated
- dealloc: the number of cumulative bytes deallocated

```
FuturesUnordered
    count:    1024002
    alloc:    36864136 B
    dealloc:  36864000 B

ConcurrentProcessQueue
    count:    260
    alloc:    20544 B
    dealloc:  0 B
```

### Conclusion

As you can see, `ConcurrentProcessQueue` massively reduces you memory overhead while providing a small performance gain. Perfect for if you want a fixed batch size
