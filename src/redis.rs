use std::fmt::Display;
use deadpool::managed::Object;
use redis::{
    aio::{ConnectionManager, ConnectionManagerConfig}, cluster::ClusterClientBuilder, cluster_async::ClusterConnection, cmd, Client, Cmd, FromRedisValue, PushInfo, RedisError, RedisResult, ToRedisArgs
};
use tokio::sync::OnceCell;

static REDIS: OnceCell<Pool> = OnceCell::const_new();
static DB_NAME: OnceCell<String> = OnceCell::const_new();

pub struct Redis {
    pub connection: RedisConnection,
}

impl Redis {
    async fn init(uri: &str) -> RedisResult<Redis> {
        let nodes = uri.split(",").collect::<Vec<&str>>();
        if nodes.len() == 1 {
            let client = Client::open(uri)?;
            let config = ConnectionManagerConfig::default().set_number_of_retries(usize::MAX);
            let connection = ConnectionManager::new_with_config(client.clone(), config).await?;
            let connection = RedisConnection::Single(connection);
            Ok(Redis { connection })
        } else {
            let client = ClusterClientBuilder::new(nodes)
                .use_protocol(redis::ProtocolVersion::RESP3)
                .retries(u32::MAX)
                .build()?;
            let connection = client.get_async_connection().await?;
            let connection = RedisConnection::Cluster(connection);
            Ok(Redis { connection })
        }
    }

    pub async fn init_pubsub(uri: &str) -> RedisResult<RedisPubsub> {
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
            Ok(RedisPubsub { connection, stream })
        } else {
            let client = ClusterClientBuilder::new(nodes)
                .use_protocol(redis::ProtocolVersion::RESP3)
                .push_sender(tx)
                .retries(u32::MAX)
                .retry_wait_formula(3000, 1)
                .build()?;
            let connection = client.get_async_connection().await?;
            let connection = RedisConnection::Cluster(connection);
            Ok(RedisPubsub { connection, stream })
        }
    }

    pub async fn instance() -> anyhow::Result<Object<Manager>> {
        let pool = REDIS.get().unwrap();
        let redis = pool.get().await?;
        Ok(redis)
    }

    /// SET key value
    pub async fn set<K, V>(key: K, value: V) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Display,
        V: ToRedisArgs,
    {
        let key = format!("{}:{}", DB_NAME.get().unwrap(), key);
        let mut redis = Redis::instance().await?;
        redis
            .connection
            .exec(cmd("SET").arg(key).arg(value))
            .await?;
        Ok(())
    }

    /// SET key value EX seconds
    pub async fn set_ex<K, V>(key: K, value: V, ex: u32) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Display,
        V: ToRedisArgs,
    {
        let key = format!("{}:{}", DB_NAME.get().unwrap(), key);
        let mut redis = Redis::instance().await?;
        redis
            .connection
            .exec(cmd("SET").arg(key).arg(value).arg("EX").arg(ex))
            .await?;
        Ok(())
    }

    /// GET key
    pub async fn get<K, V>(key: K) -> anyhow::Result<Option<V>>
    where
        K: ToRedisArgs + Display,
        V: FromRedisValue,
    {
        let key = format!("{}:{}", DB_NAME.get().unwrap(), key);
        let mut redis = Redis::instance().await?;
        let value = redis
            .connection
            .query::<Option<V>>(cmd("GET").arg(key))
            .await?;
        Ok(value)
    }

    /// HSET table key value
    pub async fn hset<K, V>(table: &str, key: K, value: V) -> anyhow::Result<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        let table = format!("{}:{}", DB_NAME.get().unwrap(), table);
        let mut redis = Redis::instance().await?;
        redis
            .connection
            .exec(cmd("HSET").arg(table).arg(key).arg(value))
            .await?;
        Ok(())
    }

    /// HSET table key2 value2 key2 value2
    pub async fn hmset<K, V>(table: &str, pairs: &[(K, V)]) -> anyhow::Result<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        let table = format!("{}:{}", DB_NAME.get().unwrap(), table);
        let mut redis = Redis::instance().await?;
        let mut command = cmd("HSET");
        command.arg(table);
        for (key, value) in pairs {
            command.arg(key);
            command.arg(value);
        }
        redis
            .connection
            .exec(&mut command)
            .await?;
        Ok(())
    }

    /// HSET table key value EX seconds
    pub async fn hset_ex<K, V>(table: &str, key: K, value: V, ex: u32) -> anyhow::Result<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        let table = format!("{}:{}", DB_NAME.get().unwrap(), table);
        let mut redis = Redis::instance().await?;
        redis
            .connection
            .exec(cmd("HSET").arg(table).arg(key).arg(value).arg("EX").arg(ex))
            .await?;
        Ok(())
    }

    /// HGET table key
    pub async fn hget<K, V>(table: &str, key: K) -> anyhow::Result<Option<V>>
    where
        K: ToRedisArgs,
        V: FromRedisValue,
    {
        let table = format!("{}:{}", DB_NAME.get().unwrap(), table);
        let mut redis = Redis::instance().await?;
        let value = redis
            .connection
            .query::<Option<V>>(cmd("HGET").arg(table).arg(key))
            .await?;
        Ok(value)
    }

    /// HMGET table key1 key2 key3
    pub async fn hmget<K, V>(table: &str, keys: &[K]) -> anyhow::Result<Vec<V>>
    where
        K: ToRedisArgs,
        V: FromRedisValue,
    {
        let table = format!("{}:{}", DB_NAME.get().unwrap(), table);
        let mut redis = Redis::instance().await?;
        let items: Vec<V> = redis
            .connection
            .query(cmd("HMGET").arg(table).arg(keys))
            .await?;
        Ok(items)
    }

    /// HGETALL table
    pub async fn hget_all<K, V>(table: &str) -> anyhow::Result<Vec<(K, V)>>
    where
        K: FromRedisValue,
        V: FromRedisValue,
    {
        let table = format!("{}:{}", DB_NAME.get().unwrap(), table);
        let mut redis = Redis::instance().await?;
        let value = redis
            .connection
            .query::<Vec<(K, V)>>(cmd("HGETALL").arg(table))
            .await?;
        Ok(value)
    }

    /// HDEL table key
    pub async fn hdel<K>(table: &str, key: K) -> anyhow::Result<()>
    where
        K: ToRedisArgs,
    {
        let table = format!("{}:{}", DB_NAME.get().unwrap(), table);
        let mut redis = Redis::instance().await?;
        redis
            .connection
            .exec(cmd("HDEL").arg(table).arg(key))
            .await?;
        Ok(())
    }

    /// HDEL table key1 key2 key3
    pub async fn hmdel<K>(table: &str, keys: &[K]) -> anyhow::Result<()>
    where
        K: ToRedisArgs,
    {
        let table = format!("{}:{}", DB_NAME.get().unwrap(), table);
        let mut redis = Redis::instance().await?;
        let mut command = cmd("HDEL");
        command.arg(table);
        for key in keys {
             command.arg(key);
        }
        redis
            .connection
            .exec(&mut command)
            .await?;
        Ok(())
    }

    /// DEL table
    pub async fn hdel_all(table: &str) -> anyhow::Result<()> {
        let table = format!("{}:{}", DB_NAME.get().unwrap(), table);
        let mut redis = Redis::instance().await?;
        redis
            .connection
            .exec(cmd("DEL").arg(table))
            .await?;
        Ok(())
    }
}

pub struct RedisPubsub {
    pub connection: RedisConnection,
    pub stream: tokio::sync::mpsc::UnboundedReceiver<PushInfo>,
}

impl RedisPubsub {
    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs + Display) -> RedisResult<()> {
        let channel_name = format!("{}:{}", DB_NAME.get().unwrap(), channel_name);
        match &mut self.connection {
            RedisConnection::Single(c) => c.subscribe(channel_name).await,
            RedisConnection::Cluster(c) => c.subscribe(channel_name).await,
        }
    }
}

#[derive(Debug)]
pub struct Manager {
    uri: String,
}

impl deadpool::managed::Manager for Manager {
    type Type = Redis;
    type Error = RedisError;

    async fn create(&self) -> Result<Redis, RedisError> {
        Redis::init(&self.uri).await
    }

    async fn recycle(
        &self,
        _: &mut Redis,
        _: &deadpool::managed::Metrics,
    ) -> deadpool::managed::RecycleResult<RedisError> {
        Ok(())
    }
}

type Pool = deadpool::managed::Pool<Manager>;

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
}

/// Initialize Redis connection
pub async fn init(uri: &str, db_name: &str) -> anyhow::Result<()> {
    tracing::info!("Initializing Redis...");
    let mgr = Manager {
        uri: uri.to_string(),
    };
    let pool = Pool::builder(mgr).build()?;
    REDIS.set(pool).map_err(|e| anyhow::anyhow!("Failed to set Redis pool: {}", e))?;
    DB_NAME.set(db_name.to_string()).map_err(|e| anyhow::anyhow!("Failed to set DB_NAME: {}", e))?;
    tracing::info!("Redis initialized with uri: {}", uri);
    Ok(())
}
