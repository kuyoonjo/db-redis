use crate::connection::RedisConnection;
use redis::{
    Client, PushInfo, RedisResult, ToRedisArgs,
    aio::{ConnectionManager, ConnectionManagerConfig},
    cluster::ClusterClientBuilder,
};
use std::fmt::Display;

pub struct RedisPubsub {
    pub connection: RedisConnection,
    pub stream: tokio::sync::mpsc::UnboundedReceiver<PushInfo>,
    pub db_name: String,
}

impl RedisPubsub {
    pub async fn init(uri: &str, db_name: &str) -> RedisResult<RedisPubsub> {
        let nodes = uri.split(",").collect::<Vec<&str>>();
        let (tx, stream) = tokio::sync::mpsc::unbounded_channel();
        if nodes.len() == 1 {
            let client = Client::open(uri)?;
            let config = ConnectionManagerConfig::default()
                .set_number_of_retries(usize::MAX)
                .set_factor(3000)
                .set_exponent_base(1)
                .set_push_sender(tx)
                .set_automatic_resubscription();
            let connection = ConnectionManager::new_with_config(client.clone(), config).await?;
            let connection = RedisConnection::Single(connection);
            Ok(RedisPubsub {
                connection,
                stream,
                db_name: db_name.to_string(),
            })
        } else {
            let client = ClusterClientBuilder::new(nodes)
                .use_protocol(redis::ProtocolVersion::RESP3)
                .push_sender(tx)
                .retries(u32::MAX)
                .retry_wait_formula(3000, 1)
                .build()?;
            let connection = client.get_async_connection().await?;
            let connection = RedisConnection::Cluster(connection);
            Ok(RedisPubsub {
                connection,
                stream,
                db_name: db_name.to_string(),
            })
        }
    }

    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs + Display) -> RedisResult<()> {
        let channel_name = format!("{}:{}", self.db_name, channel_name);
        match &mut self.connection {
            RedisConnection::Single(c) => c.subscribe(channel_name).await,
            RedisConnection::Cluster(c) => c.subscribe(channel_name).await,
        }
    }

    pub async fn unsubscribe(
        &mut self,
        channel_name: impl ToRedisArgs + Display,
    ) -> RedisResult<()> {
        let channel_name = format!("{}:{}", self.db_name, channel_name);
        match &mut self.connection {
            RedisConnection::Single(c) => c.unsubscribe(channel_name).await,
            RedisConnection::Cluster(c) => c.unsubscribe(channel_name).await,
        }
    }
}
