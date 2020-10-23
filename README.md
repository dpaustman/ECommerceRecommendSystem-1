# 大数据实战电商推荐系统

​		项目以推荐系统建设领域知名的经过修改过的中文亚马逊电商数据集作为依托，以某电商网站真实业务数据架构为基础，构建了符合教学体系的一体化的电商推荐系统，包含了离线推荐与实时推荐体系，综合利用了协同过滤算法以及基于内容的推荐方法来提供混合推荐。

#### 技术

|                 |
| --------------- |
| MongoDB         |
| Redis           |
| Spark SQL       |
| Spark Streaming |
| Spark MLlib     |
| Flume           |
| Kafka           |

---

#### 数据加载初始化

##### 模块__dataLoader

​	通过Spark SQL将系统初始化数据加载到MongoDB中。

#### 离线推荐

##### 模块__StatisticsRecommender

​	离线统计服务：批处理统计性业务采用Spark Core + Spark SQL进行实现，实现对指标类数据的统计任务。

##### 模块__OfflineRecommender

​	离线推荐服务：离线推荐业务采用Spark Core + Spark MLlib进行实现，采用ALS算法进行实现。

#### 实时推荐

##### 模块__OnlineRecommender

​	实时推荐服务：项目采用Spark Streaming作为实时推荐系统，通过接收Kafka中缓存的数据，通过设计的推荐算法实现对实时推荐的数据处理，并将结构合并更新到MongoDB数据库。

##### 模块__KafkaStreaming

​	消息缓冲服务：项目采用Kafka作为流式数据的缓存组件，接受来自Flume的数据采集请求。并将数据推送到项目的实时推荐系统部分。

##### 模块__businessServer

​	日志采集服务：通过利用Flume-ng对业务平台中用户对于商品的一次评分行为进行采集，实时发送到Kafka集群。

#### 其它形式的离线相似推荐服务

##### 模块__ContentRecommender

​	基于内容的相似推荐：通过用户给商品打上的标签，我们将标签内容进行提取，得到商品的内容特征向量，进而可以通过求取相似度矩阵。与实时推荐系统直接对接，计算出与用户当前评分商品的相似商品，实现基于内容的实时推荐。

##### 模块__ItemCFRecommender

​	基于物品的协同过滤相似推荐：通过收集用户的常规行为数据（比如点击、收藏、购买）就可以得到商品间的相似度，利用已有的行为数据，分析商品受众的相似程度，进而得出商品间的相似度。

---

#### 程序部署与运行

- 编译项目：执行root项目的clean package阶段

![](.\clean.jpg)

![](.\package.jpg)

- 安装前端项目

  将website-release.tar.gz解压到/var/www/html目录下，将里面的文件放在根目录

- 安装业务服务器

  将BusinessServer.war，放到tomcat的webapp目录下，并将解压出来的文件，放到ROOT目录下

- Kafka配置与启动

  在kafka中创建两个Topic，一个为log，一个为recommender

  启动kafkaStream程序，用于在log和recommender两个topic之间进行数据格式化

- Flume配置与启动

  在flume安装目录下的conf文件夹下，创建log-kafka.properties

  ```properties
  agent.sources = exectail
  agent.channels = memoryChannel
  agent.sinks = kafkasink
  
  # For each one of the sources, the type is defined
  agent.sources.exectail.type = exec
  agent.sources.exectail.command = tail -f /opt/moudle/apache-tomcat-8.5.23/logs/catalina.out
  agent.sources.exectail.interceptors=i1
  agent.sources.exectail.interceptors.i1.type=regex_filter
  agent.sources.exectail.interceptors.i1.regex=.+PRODUCT_RATING_PREFIX.+
  # The channel can be defined as follows.
  agent.sources.exectail.channels = memoryChannel
  
  # Each sink's type must be defined
  agent.sinks.kafkasink.type = org.apache.flume.sink.kafka.KafkaSink
  agent.sinks.kafkasink.kafka.topic = log
  agent.sinks.kafkasink.kafka.bootstrap.servers = linux:9092
  agent.sinks.kafkasink.kafka.producer.acks = 1
  agent.sinks.kafkasink.kafka.flumeBatchSize = 20
  
  
  #Specify the channel the sink should use
  agent.sinks.kafkasink.channel = memoryChannel
  
  # Each channel's type is defined.
  agent.channels.memoryChannel.type = memory
  
  # Other config values specific to each type of channel(sink or source)
  # can be defined as well
  # In this case, it specifies the capacity of the memory channel
  agent.channels.memoryChannel.capacity = 10000
  ```

  启动flume

  ```sh
  [kevin@hadoop101 apache-flume-1.7.0-kafka]$ bin/flume-ng agent -c ./conf/ -f ./conf/log-kafka.properties -n agent
  ```

- 部署流式计算服务

  提交SparkStreaming程序

  ```sh
  [kevin@hadoop101 spark-2.1.1-bin-hadoop2.7]$ bin/spark-submit --class com.atguigu.streamingRecommender.StreamingRecommender streamingRecommender-1.0-SNAPSHOT.jar
  ```

- Azkaban调度离线算法

  创建两个job文件如下

  Azkaban-stat.job

  ```
  type=command
  command=/home/bigdata/cluster/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class com.atguigu.offline.RecommenderTrainerApp
   offlineRecommender-1.0-SNAPSHOT.jar
  ```

  Azkaban-offline.job

  ```
  type=command
  command=/home/bigdata/cluster/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class com.atguigu.statisticsRecommender.StatisticsApp
   statisticsRecommender-1.0-SNAPSHOT.jar
  ```

  将Job文件打成ZIP包上传到azkaban。

