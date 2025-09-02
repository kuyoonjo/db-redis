use redis::{
    AsyncCommands, Cmd, FromRedisValue, RedisResult, ToRedisArgs, aio::ConnectionManager,
    cluster_async::ClusterConnection,
};

pub enum RedisConnection {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

impl RedisConnection {
    pub async fn query<T: FromRedisValue>(&mut self, cmd: &mut Cmd) -> RedisResult<T> {
        match self {
            RedisConnection::Single(client) => cmd.query_async(client).await,
            RedisConnection::Cluster(client) => cmd.query_async(client).await,
        }
    }
    pub async fn exec(&mut self, cmd: &mut Cmd) -> RedisResult<()> {
        match self {
            RedisConnection::Single(client) => cmd.exec_async(client).await,
            RedisConnection::Cluster(client) => cmd.exec_async(client).await,
        }
    }
    pub async fn publish<C, M>(&mut self, channel: C, message: M) -> RedisResult<()>
    where
        C: ToRedisArgs + Send + Sync,
        M: ToRedisArgs + Send + Sync,
    {
        match self {
            RedisConnection::Single(client) => client.publish(channel, message).await?,
            RedisConnection::Cluster(client) => client.publish(channel, message).await?,
        }
        Ok(())
    }
}
