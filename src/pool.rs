use deadpool::managed::Object;
use redis::{
    Client, FromRedisValue, RedisError, RedisResult, ToRedisArgs,
    aio::{ConnectionManager, ConnectionManagerConfig},
    cluster::ClusterClientBuilder,
    cmd,
};
use std::fmt::Display;
use crate::connection::RedisConnection;

pub struct Redis {
    pub connection: RedisConnection,
    pub db_name: String,
}

impl Redis {
    pub async fn init(uri: &str, db_name: &str) -> RedisResult<Redis> {
        let nodes = uri.split(",").collect::<Vec<&str>>();
        if nodes.len() == 1 {
            let client = Client::open(uri)?;
            let config = ConnectionManagerConfig::default().set_number_of_retries(usize::MAX);
            let connection = ConnectionManager::new_with_config(client.clone(), config).await?;
            let connection = RedisConnection::Single(connection);
            Ok(Redis {
                connection,
                db_name: db_name.to_string(),
            })
        } else {
            let client = ClusterClientBuilder::new(nodes)
                .use_protocol(redis::ProtocolVersion::RESP3)
                .retries(u32::MAX)
                .build()?;
            let connection = client.get_async_connection().await?;
            let connection = RedisConnection::Cluster(connection);
            Ok(Redis {
                connection,
                db_name: db_name.to_string(),
            })
        }
    }
}

#[derive(Debug)]
pub struct Manager {
    uri: String,
    db_name: String,
}

impl deadpool::managed::Manager for Manager {
    type Type = Redis;
    type Error = RedisError;

    async fn create(&self) -> Result<Redis, RedisError> {
        Redis::init(&self.uri, &self.db_name).await
    }

    async fn recycle(
        &self,
        _: &mut Redis,
        _: &deadpool::managed::Metrics,
    ) -> deadpool::managed::RecycleResult<RedisError> {
        Ok(())
    }
}

pub struct RedisPool(pub deadpool::managed::Pool<Manager>);

impl RedisPool {
    pub fn init(uri: &str, db_name: &str) -> anyhow::Result<Self> {
        let manager = Manager {
            uri: uri.to_string(),
            db_name: db_name.to_string(),
        };
        let pool = deadpool::managed::Pool::<Manager>::builder(manager).build()?;
        Ok(Self(pool))
    }

    pub async fn instance(&self) -> anyhow::Result<Object<Manager>> {
        let redis = self.0.get().await?;
        Ok(redis)
    }

    /// SET key value
    pub async fn set<K, V>(&self, key: K, value: V) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Display,
        V: ToRedisArgs,
    {
        let mut redis = self.instance().await?;
        let key = format!("{}:{}", redis.db_name, key);
        redis
            .connection
            .exec(cmd("SET").arg(key).arg(value))
            .await?;
        Ok(())
    }

    /// SET key value EX seconds
    pub async fn set_ex<K, V>(&self, key: K, value: V, ex: u32) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Display,
        V: ToRedisArgs,
    {
        let mut redis = self.instance().await?;
        let key = format!("{}:{}", redis.db_name, key);
        redis
            .connection
            .exec(cmd("SET").arg(key).arg(value).arg("EX").arg(ex))
            .await?;
        Ok(())
    }

    /// GET key
    pub async fn get<K, V>(&self, key: K) -> anyhow::Result<Option<V>>
    where
        K: ToRedisArgs + Display,
        V: FromRedisValue,
    {
        let mut redis = self.instance().await?;
        let key = format!("{}:{}", redis.db_name, key);
        let value = redis
            .connection
            .query::<Option<V>>(cmd("GET").arg(key))
            .await?;
        Ok(value)
    }

    /// HSET table key value
    pub async fn hset<K, V>(&self, table: &str, key: K, value: V) -> anyhow::Result<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        let mut redis = self.instance().await?;
        let table = format!("{}:{}", redis.db_name, table);
        redis
            .connection
            .exec(cmd("HSET").arg(table).arg(key).arg(value))
            .await?;
        Ok(())
    }

    /// HSET table key2 value2 key2 value2
    pub async fn hmset<K, V>(&self, table: &str, pairs: &[(K, V)]) -> anyhow::Result<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        let mut redis = self.instance().await?;
        let table = format!("{}:{}", redis.db_name, table);
        let mut command = cmd("HSET");
        command.arg(table);
        for (key, value) in pairs {
            command.arg(key);
            command.arg(value);
        }
        redis.connection.exec(&mut command).await?;
        Ok(())
    }

    /// HSET table key value EX seconds
    pub async fn hset_ex<K, V>(&self, table: &str, key: K, value: V, ex: u32) -> anyhow::Result<()>
    where
        K: ToRedisArgs,
        V: ToRedisArgs,
    {
        let mut redis = self.instance().await?;
        let table = format!("{}:{}", redis.db_name, table);
        redis
            .connection
            .exec(cmd("HSET").arg(table).arg(key).arg(value).arg("EX").arg(ex))
            .await?;
        Ok(())
    }

    /// HGET table key
    pub async fn hget<K, V>(&self, table: &str, key: K) -> anyhow::Result<Option<V>>
    where
        K: ToRedisArgs,
        V: FromRedisValue,
    {
        let mut redis = self.instance().await?;
        let table = format!("{}:{}", redis.db_name, table);
        let value = redis
            .connection
            .query::<Option<V>>(cmd("HGET").arg(table).arg(key))
            .await?;
        Ok(value)
    }

    /// HMGET table key1 key2 key3
    pub async fn hmget<K, V>(&self, table: &str, keys: &[K]) -> anyhow::Result<Vec<V>>
    where
        K: ToRedisArgs,
        V: FromRedisValue,
    {
        let mut redis = self.instance().await?;
        let table = format!("{}:{}", redis.db_name, table);
        let items: Vec<V> = redis
            .connection
            .query(cmd("HMGET").arg(table).arg(keys))
            .await?;
        Ok(items)
    }

    /// HGETALL table
    pub async fn hget_all<K, V>(&self, table: &str) -> anyhow::Result<Vec<(K, V)>>
    where
        K: FromRedisValue,
        V: FromRedisValue,
    {
        let mut redis = self.instance().await?;
        let table = format!("{}:{}", redis.db_name, table);
        let value = redis
            .connection
            .query::<Vec<(K, V)>>(cmd("HGETALL").arg(table))
            .await?;
        Ok(value)
    }

    /// HDEL table key
    pub async fn hdel<K>(&self, table: &str, key: K) -> anyhow::Result<()>
    where
        K: ToRedisArgs,
    {
        let mut redis = self.instance().await?;
        let table = format!("{}:{}", redis.db_name, table);
        redis
            .connection
            .exec(cmd("HDEL").arg(table).arg(key))
            .await?;
        Ok(())
    }

    /// HDEL table key1 key2 key3
    pub async fn hmdel<K>(&self, table: &str, keys: &[K]) -> anyhow::Result<()>
    where
        K: ToRedisArgs,
    {
        let mut redis = self.instance().await?;
        let table = format!("{}:{}", redis.db_name, table);
        let mut command = cmd("HDEL");
        command.arg(table);
        for key in keys {
            command.arg(key);
        }
        redis.connection.exec(&mut command).await?;
        Ok(())
    }

    /// DEL table
    pub async fn hdel_all(&self, table: &str) -> anyhow::Result<()> {
        let mut redis = self.instance().await?;
        let table = format!("{}:{}", redis.db_name, table);
        redis.connection.exec(cmd("DEL").arg(table)).await?;
        Ok(())
    }

    pub async fn publish<C, M>(&self, channel: C, message: M) -> anyhow::Result<()>
    where
        C: ToRedisArgs + Display,
        M: ToRedisArgs + Send + Sync,
    {
        let mut redis = self.instance().await?;
        let channel = format!("{}:{}", redis.db_name, channel);
        redis
            .connection
            .publish(channel.as_bytes(), message)
            .await?;
        Ok(())
    }
}
