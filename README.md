# Usage

## Rust

```rust
use db_redis::Redis;

db_redis::init("redis://127.0.0.1?protocol=resp3", "test").await?;
Redis::set("key", "value").await?;

let pubsub = Redis::init_pubsub("redis://127.0.0.1?protocol=resp3").await?;
pubsub.subscribe("channel").await?;
while let Some(push_info) = pubsub.stream.recv().await {
    match push_info.kind {
        redis::PushKind::Message => {
            tracing::info!("{:?}", push_info.data);
        }
        _ => { }
    }
}
```
