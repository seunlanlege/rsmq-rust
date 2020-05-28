A Rust implementation of [RSMQ](https://smrchy.github.io/rsmq/about/) (Redis Simple Message Queue).

## Installation

Add this line to your application's Cargo.toml:

```toml
[dependencies]
rsmq = "*"
```

## Usage

```rust
use rsmq::*;

#[tokio::main]
fn main() {
  let rsmq = Rsmq::new("redis://127.0.0.1/").await.expect("Can't connect to Redis");
  let qopts = Queue {
    qname: "my-queue".into(),
    vt: 60,
    delay: 120,
	maxsize: 3000,
	..Default::default()
  };
  rsmq.create_queue(qopts).await.expect("queue creation failed");
  let qs = rsmq.list_queues().await.expect("Nope, no listing for you");
  println!("List queues: {:?}", qs);
  rsmq.delete_queue("my-queue").await.expect("q deletion failed");
}
```
## Contributing

1. Fork it ( http://github.com/dvdplm/rsmq-rust )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request