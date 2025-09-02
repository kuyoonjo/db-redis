# Usage

## Rust

```rust
use db_redis::{RedisPool, RedisPubsub};

let pool = RedisPool::init("redis://127.0.0.1?protocol=resp3", "test")?;
pool.set("key", "value").await?;

let pubsub = RedisPubsub::init("redis://127.0.0.1?protocol=resp3").await?;
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
