---
title: spark向codis写入千万级数据
tags: ['spark','codis','etl']
---

公司里使用codis来搭建分布式redis集群，在最近的项目中遇到了向codis批量导入数据的问题，由于数据源可以是hive或者mysql，而我想试试用spark来导入，于是简单尝试了一下。



## 使用原生的spark-redis库

在网上查到了一个官方的库，叫[spark-redis](https://github.com/RedisLabs/spark-redis)

使用`spark-redis`使用简单配置即可写入redis

``` scala
    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .save()
```

### spark-redis的局限

1. 使用`spark-redis`插入的格式默认是redis hash，且插入的key名默认为`table:redis-hash` 的形式，不是特别灵活。
2. **大问题是**由于codis是豌豆荚的开源库，在国外使用率不高，所以`spark-redis`这个库虽然也是使用`jedis`进行连接，但是并不能直接访问codis的proxy，会报错

`redis.clients.jedis.exceptions.JedisDataException: ERR backend server 'Replication' not found`

看了`spark-redis`源码之后发现这个库确实是没法连接codis

于是我试着直接往codis里的某个节点写(千万不要学我)，成功把一个节点的replica写挂了￣□￣｜｜

## 使用jedis+mapPartition

上述方法失败之后，我试着转向原生的spark rdd层面来实现。

### 首先解决连接问题

其实jedis是可以连接codis的，使用`RoundRobinJedisPool`可以通过zk路径直接连接codis，这里简单实现了一个连接类

``` scala
object InternalRedisClient extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass)
  @transient private var pool: RoundRobinJedisPool = null

  def makePool(zkAddr: String, maxTotal: Int, maxIdle: Int, minIdle: Int, database: String): JedisResourcePool = {
    if (pool == null) {
      val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig();
      jedisPoolConfig.setMaxTotal(maxTotal);
      jedisPoolConfig.setMaxIdle(maxIdle);
      jedisPoolConfig.setMaxWaitMillis(10000);
      jedisPoolConfig.setTestOnBorrow(true);
      jedisPoolConfig.setTestOnReturn(true);
      pool = RoundRobinJedisPool
        .create()
        .curatorClient(zkAddr, 10000)
        .zkProxyDir("/zk/codis/" + database + "/proxy")
        .poolConfig(jedisPoolConfig)
        .build()
      logger.warn("+++++create new  pool+++++")
      val hook = new Thread {
        override def run = pool.close()
      }
      sys.addShutdownHook(hook.run)
    }
    pool
  }
}
```

其中`zkAddr`是zk的路径，比如说`xxxx:2181,yyyy:2181,zzzz:2181`，`zkProxyDir`是codis在zk里写的路径，`database`是codis的集群名

### 接着使用spark rdd写入codis

redis的写入是单线程的，但是codis proxy可以把写入key的操作hash到各个节点，所以codis可以比单机redis更快的写入，这里利用spark分布式的优势，在每个partition里写入，代码如下

``` scala
  def writeDataToRedis(df: DataFrame) = {
    df.repartition(4000)
      .foreachPartition(rows => {
        val pool = InternalRedisClient.makePool(zkPath, -1, -1, -1, codisDB)
        val jedis = pool.getResource
        val pipelined: Pipeline = jedis.pipelined()
        val random = new Random();
        var count = 0
        rows.foreach(r => {
          count += 1
          if (count % 1000 == 0) Thread.sleep((random.nextDouble() * 200).toInt)
          val key = r.getString(0)
          val value = r.getString(1)
          pipelined.setex(key, 604800, value)
        })
        pipelined.close()
        jedis.close()
      })
  }
```

这里有几个技巧

1. 由于`foreachPartition`会在每个pairition上(其实是excutor里)建立一个对codis的session，所以子啊写入之前先repartition，让数据在每个partition里均匀分布
2. 随机`Thread.sleep()`,让这些excutor能在不同时间连接codis
3. 使用`pipelined.setex`，这个方法是一个原子操作，同时写入和设置过期时间
4. 在spark-submit中，通过调节参数可以控制写入速度

```shell
  --num-executors 16\
  --executor-cores 8\
  --executor-memory 4g \
  --driver-memory 10g \
  --queue root.hdfs \
  --conf spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45 \
  --conf spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45 \
  --conf spark.default.parallelism=200 \
```

通过调整`executor-cores`和`num-executors`,来控制连接到codis的sessions数量

### 写入qps和速度

实测通过调整spark-submit中的参数，让session控制在200个左右，写入qps控制在50w左右，可以在不到2分钟的时候插入超过5000万条数据，非常的快速和方便。

## 参考链接

> http://bourneli.github.io/scala/spark/pit/2017/10/27/spark-to-redis.html