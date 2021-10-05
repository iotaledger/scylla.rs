## Examples
### Scylla
```sh
$ RUST_LOG=info cargo run --example scylla
```

### Benchmark

```sh
$ SCYLLA_NODE=172.17.0.2:19042 RUST_LOG=info cargo run --example benchmark --features="backstage/backserver" --release
```
