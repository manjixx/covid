# 一、软件安装

> ## MacOS 安装 Centos虚拟机

### 基本配置

- MacBook Air (M1, 2020)
- 操作系统: Mac OS Monterey 12.4
- 虚拟机：VMware Fusion 专业版 e.x.p (19431034),官网目前可白嫖
  
  下载链接：[M1版本VMware Fusion](https://customerconnect.vmware.com/downloads/get-download?downloadGroup=FUS-PUBTP-2021H1)

### Centos 8 安装包下载

- 下载CentOS 8安装包，注意Centos 7 貌似无法在Mac M1上进行安装,系统提示架构问题
- [Centos 8 百度网盘](https://pan.baidu.com/s/1xSQ8ykESVOPTwBJ5c8xFDQ?pwd=kyqb) 提取码:kyqb

### Centos 8安装

参考下列文章完成系统安装与配置

[Centos 8安装教程](https://blog.csdn.net/qq_42778369/article/details/123093854)

[Centos 8安装，配置以及开启SSH](https://blog.csdn.net/qq_24950043/article/details/122517521?spm=1001.2101.3001.6661.1&utm_medium=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7ECTRLIST%7ERate-1-122517521-blog-123093854.pc_relevant_antiscanv2&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7ECTRLIST%7ERate-1-122517521-blog-123093854.pc_relevant_antiscanv2&utm_relevant_index=1)

[Centos 8配置阿里镜像源](https://blog.csdn.net/xiaocao_debug/article/details/123041407)

> ## [伪集群搭建](https://blog.csdn.net/yuanzhengme/article/details/119461842)

### 1.在Centos上安装JAVA

- 下载[Java 8安装包](https://www.oracle.com/java/technologies/downloads/#java8) ,注意查看系统版本与安装包版本是否匹配

```shell
uname -a
Linux centos8 5.11.12-300.el8.aarch64 #1 SMP Fri Jul 30 12:03:15 CST 2021 aarch64 aarch64 aarch64 GNU/Linux
```

- 将安装包上传至Centos 8虚拟机```/usr/local```目录下，并解压

```shell
tar -zxvf jdk-8u333-linux-aarch64.tar.gz
```

- 删除CentOS自带的java

```shell
rm /usr/bin/java
```

- 创建新的链接,并查看Java版本

```shell
ln -s /usr/local/jdk1.8.0_333/bin/java /usr/bin/java
java -version
```

- 配置环境变量,并在profile文件中添加如下内容,注意安装版本不同，文件路径不同

```shell
vim /etc/profile
#不同版本文件夹名称不同
JAVA_HOME=/usr/local/jdk1.8.0_333
JRE_HOME=/usr/local/jdk1.8.0_333/jre
PATH=$PATH:$JAVA_HOME/bin:$JRE_HOME/bin
CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar:$JRE_HOME/lib
export JAVA_HOME JRE_HOME CLASSPATH PATH USER LOGNAME MAIL HOSTNAME HISTSIZE HISTCONTROL
```

- 执行```source /etc/profile```，使得刚刚对profile的修改生效
- 执行命令：```jps```,  jps（java virtual machine process status tool）是java提供的一个用来显示当前所有java进程及其pid的命令，非常简单实用。
  通过它，可以查看当前到底启动了几个java进程，因为每一个java程序都会独占一个java虚拟机实例。

```shell
jps
126149 Jps
```

### 2. Zookeeper 安装(一台服务器实现三个节点的ZooKeeper集群)

#### 环境说明

- 系统 centos 8
- java-8
- zookeeper 3.7.1

#### 前期准备

- 主机名与ip地址映射，随后的配置文件内会使用主机名来映射ip地址,注意此需要使用本机器ip地址和主机名

```shell
vim /etc/hosts
192.168.29.129 centos8
```

- zookeeper需要在Java-8以上的版本运行,因此需要首先安装Java-8

```shell
[root@centos8 bin]# java -version
java version "1.8.0_333"
Java(TM) SE Runtime Environment (build 1.8.0_333-b02)
Java HotSpot(TM) 64-Bit Server VM (build 25.333-b02, mixed mode)
```

- 查看防火墙状态，如果为开启状态需要关闭

```shell
# 查看防火墙状态
[root@centos8 ~]# systemctl status firewalld.service
● firewalld.service - firewalld - dynamic firewall daemon
   Loaded: loaded (/usr/lib/systemd/system/firewalld.service; enabled; vendor preset: enabled)
   Active: active (running) since Mon 2022-06-06 01:04:18 CST; 4h 24min left
     Docs: man:firewalld(1)
 Main PID: 874 (firewalld)
    Tasks: 2 (limit: 3865)
   Memory: 33.6M
   CGroup: /system.slice/firewalld.service
           └─874 /usr/libexec/platform-python -s /usr/sbin/firewalld --nofork --nopid

6月 06 01:04:15 centos8 systemd[1]: Starting firewalld - dynamic firewall daemon...
6月 06 01:04:18 centos8 systemd[1]: Started firewalld - dynamic firewall daemon.
6月 06 01:04:18 centos8 firewalld[874]: WARNING: AllowZoneDrifting is enabled. This is considered an insecure configuration option. It will be removed in a future release. Please consider disabling it now.

# 关闭防火墙
[root@centos8 ~]# systemctl stop firewalld.service
```

#### 安装配置

- 准备伪集群代码目录

```shell
# 准备伪集群目录【zookeeper-cluster 数据文件夹 日志文件夹】【这里不知道有没有简单方法 知道的小伙伴在评论区分享一下 造福大家】
mkdir /usr/local/zookeeper-cluster
cd /usr/local/zookeeper-cluster
mkdir data log
cd /usr/local/zookeeper-cluster/data
mkdir 01 02 03
cd /usr/local/zookeeper-cluster/log
mkdir 01 02 03
```

- 下载[Zookeeper-3.7.1安装包](https://dlcdn.apache.org/zookeeper/zookeeper-3.7.1/apache-zookeeper-3.7.1-bin.tar.gz), 并上传至```/home```文件夹下，并解压

```shell
[root@centos8 home]# tar -zxvf apache-zookeeper-3.7.1-bin.tar.gz
```

- 分别复制zookeeper程序文件到zookeeper01 zookeeper02 zookeeper03

```shell
[root@centos8 ~]# cp -r /home/apache-zookeeper-3.7.1-bin/. /usr/local/zookeeper-cluster/zookeeper01
[root@centos8 ~]# cp -r /home/apache-zookeeper-3.7.1-bin/. /usr/local/zookeeper-cluster/zookeeper02
[root@centos8 ~]# cp -r /home/apache-zookeeper-3.7.1-bin/. /usr/local/zookeeper-cluster/zookeeper03
```

- 进入每个文件夹路径下拷贝配置样本 zoo_sample.cfg 为 zoo.cfg 并进行修改

```shell
[root@centos8 zookeeper-cluster]# cd zookeeper01/conf/
[root@centos8 conf]# cp zoo_sample.cfg  zoo.cfg
[root@centos8 conf]# vim zoo.cfg 


# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial 
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between 
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
# do not use /tmp for storage, /tmp here is just 
# example sakes.
# 另外两台 02 03
dataDir=/usr/local/zookeeper-cluster/data/01
dataLogDir=/usr/local/zookeeper-cluster/log/01
# the port at which the clients will connect
# 另外两台 2182 2183
clientPort=2181
# the maximum number of client connections.
# increase this if you need to handle more clients
#maxClientCnxns=60
#
# 【如果是多台服务器 则集群中每个节点通讯端口和选举端口可相同 伪分布式不能相同】
server.1=centos8:2287:3387
server.2=centos8:2288:3388
server.3=centos8:2289:3389
```

- 标识节点

分别在三个节点的数据存储目录下新建 myid 文件，并写入对应的节点标识。Zookeeper 集群通过myid 文件识别集群节点，并通过上文配置的节点通信端口和选举端口来进行节点通信，选举出 leader节点。

```shell
echo "1" > /usr/local/zookeeper-cluster/data/01/myid
echo "2" > /usr/local/zookeeper-cluster/data/02/myid
echo "3" > /usr/local/zookeeper-cluster/data/03/myid
```

- 启动集群

```shell
/usr/local/zookeeper-cluster/zookeeper01/bin/zkServer.sh start
/usr/local/zookeeper-cluster/zookeeper02/bin/zkServer.sh start
/usr/local/zookeeper-cluster/zookeeper03/bin/zkServer.sh start

# 【这里只贴出其中一个节点的成功日志】
ZooKeeper JMX enabled by default
Using config: /usr/local/zookeeper-cluster/zookeeper01/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED
```

- 集群验证

使用 jps 查看进程，可看到有三个zookeeper进程：

```shell
[root@centos8 data]# jps -l
299588 org.apache.zookeeper.server.quorum.QuorumPeerMain
299905 org.apache.zookeeper.server.quorum.QuorumPeerMain
299128 org.apache.zookeeper.server.quorum.QuorumPeerMain
304634 sun.tools.jps.Jps
```

使用 zkServer.sh status 查看集群各个节点状态：

```shell
/usr/local/zookeeper-cluster/zookeeper01/bin/zkServer.sh status
/usr/local/zookeeper-cluster/zookeeper02/bin/zkServer.sh status
/usr/local/zookeeper-cluster/zookeeper03/bin/zkServer.sh status

# 可以看到myid是2的是leader 1和3是follower【初始化的选举机制可自行了解一下】
/usr/local/zookeeper-cluster/zookeeper01/bin/zkServer.sh status
ZooKeeper JMX enabled by default
Using config: /usr/local/zookeeper-cluster/zookeeper01/bin/../conf/zoo.cfg
Client port found: 2181. Client address: localhost.
Mode: follower

/usr/local/zookeeper-cluster/zookeeper02/bin/zkServer.sh status
ZooKeeper JMX enabled by default
Using config: /usr/local/zookeeper-cluster/zookeeper02/bin/../conf/zoo.cfg
Client port found: 2182. Client address: localhost.
Mode: leader

/usr/local/zookeeper-cluster/zookeeper03/bin/zkServer.sh status
ZooKeeper JMX enabled by default
Using config: /usr/local/zookeeper-cluster/zookeeper03/bin/../conf/zoo.cfg
Client port found: 2183. Client address: localhost.
Mode: follower
```

### 3.基于 zookeeper 搭建高可用伪集群（一台服务器实现三个节点的 Kafka 集群）

#### 环境说明

- 系统 centos 8
- java-8
- zookeeper 3.7.1

#### 安装kafka

- 下载kafka[kafka_2.12-3.2.0.tgz](https://dlcdn.apache.org/kafka/3.2.0/kafka_2.12-3.2.0.tgz)
- 上传至虚拟机，解压并将文件移动至```/usr/local```路径下

```shell
[root@centos8 home]# tar -zxvf kafka_2.13-3.2.0.tgz 
[root@centos8 home]# mv ./kafka_2.13-3.2.0/ /usr/local/kafka
```

- 配置环境变量，为了后续方便可以 **随时随地执行命令**,统一将环境变量放在```/etc/profile.d/my_env.sh```

```shell
# 配置环境变量：
vim /etc/profile.d/my_env.sh

# 按i进入编辑模式，输入下述内容
# 添加 KAFKA_HOME和bin
export KAFKA_HOME=/usr/local/kafka
export PATH=$PATH:$KAFKA_HOME/bin
# 按ESC退出编辑模式
# 输入 :wq 退出vim

# 使得配置的环境变量立即生效：
# 首先是要赋权限【只操作一次就行】
chmod +x /etc/profile.d/my_env.sh

source /etc/profile.d/my_env.sh
```

#### 分布式配置

- 进入```{KAFKA_HOME}/config/``` 目录下 ，拷贝三份 server.properties 配置文件：

```shell
cp ./server.properties ./server-1.properties
cp ./server.properties ./server-2.properties
cp ./server.properties ./server-3.properties
```

- 查看Kafka伪集群的地址

```shell
# 将01 依次替换为02 03查看伪集群地址
/usr/local/zookeeper-cluster/zookeeper01/bin/zkServer.sh status
# 得到如下信息
Client port found: 2181. Client address: localhost.
Client port found: 2182. Client address: localhost.
Client port found: 2183. Client address: localhost.
```

- 依次修改三份server.properties 配置文件,

```shell
# 分别修改三个 server.properties 配置文件【以第一个为例】
vim /usr/local/kafka/config/server-1.properties
# - - - - - - 内容如下 - - - - - - 
# centos8 需要替换为 /etc/hosts文件中与ip地址对应的主机名称
# 1.集群中每个节点的唯一标识
#三个配置文件不同 server-2 为1 server-3 为2
broker.id=0
# 2.监听地址
# 三个配置文件不同 server-2 为9092 server-3 为9093
listeners=PLAINTEXT://centos8:9091
# 3.数据的存储位置
# 三个配置文件不同 server-2 为02 server-3 为03
log.dirs=/usr/local/kafka/kafka-logs/01
# zookeeper集群地址【这里搭建的是伪集群】
# 4.三个配置文件相同
zookeeper.connect=centos8:2181,centos8:2182,centos8:2183
```

kafka的 log.dirs 指的是数据日志的存储位置，就是分区数据的存储位置，而不是程序运行日志信息的位置。配置程序运行日志信息的位置是通过同一目录下的 log4j.properties 进行的。至此，集群配置已完成。

#### 启动集群并测试

- 由于配置过环境变量 KAFKA_HOME 所以在任何文件夹下都可以进行启动

> 注意：使用三个不同的配置文件分别启动三个实例【使用三个不同的终端窗口 且启动后不要关闭 否则服务就会停止】

```shell
kafka-server-start.sh /usr/local/kafka/config/server-1.properties
kafka-server-start.sh /usr/local/kafka/config/server-2.properties
kafka-server-start.sh /usr/local/kafka/config/server-3.properties
```

- 使用上述命令出现如下报错时，进行如下操作

```shell
[root@centos8 ~]# kafka-server-start.sh /usr/local/kafka/config/server-1.properties 
Error: VM option 'UseG1GC' is experimental and must be enabled via -XX:+UnlockExperimentalVMOptions.
Error: Could not create the Java Virtual Machine.
Error: A fatal exception has occurred. Program will exit.
# 进入如下路径
[root@centos8 ~]# cd /usr/local/kafka/bin/
# 使用vim工具
[root@centos8 bin]# vim kafka-run-class.sh
# 按i进入编辑模式，删除 -XX:+UseG1GC
KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true"
```

#### 进行测试

```shell
# 创建一个主题
#注意命令行中 centos8 一定要与之前在 /etc/hosts与主机ip地址对应的主机名称一致
[root@centos8 ~]# kafka-topics.sh --create --bootstrap-server centos8:9092 \
--replication-factor 3 \
--partitions 1 \
--topic yuan

# 查看主题信息,
# 注意命令行中 centos8 一定要与之前在 /etc/hosts与主机ip地址对应的主机名称一致
[root@centos8 ~]# kafka-topics.sh --describe --bootstrap-server centos8:9092 --topic yuan
Topic: yuan     TopicId: af2NpFH6SYuHq4mM1C17RQ PartitionCount: 1       ReplicationFactor: 3    Configs: segment.bytes=1073741824
        Topic: yuan     Partition: 0    Leader: 1       Replicas: 1,2,0 Isr: 1,2,0
```

# 二、数据爬虫和生成

> ## java 爬虫入门

### 环境准备

- 新建maven工程
- 配置pom.xml文件
- 新建crawler\src\main\resources\log4j.properties，并配置

### 数据爬取

#### 1. 使用URLConnection进行爬虫

- URLConnection请求一个URL地址，然后获取流信息，通过对流信息的操作，可以获得请求到的实体内容
- 流程
  - 1. 确定要访问/爬取URL
  - 2. 获取连接请求
  - 3. 设置连接信息：请求方式/请求参数/请求头
  - 4. 获取数据
  - 5. 关闭资源
  
#### 2. 使用HttpClient进行爬虫

- 流程
  - 1. 创建HttpClient对象
  - 2. 创建HttpPost/HttpGet对象
  - 3. 发起请求
  - 4. 判断响应状态码并获取数据
  - 5. 关闭资源
  
#### 3. HttpClient连接池的使用

- 流程
  - 1. 创建HttpClient连接池
  - 2. 设置连接池参数
  - 3. 从连接池获取HttpClient对象
  - 4. 创建HttpGet对象
  - 5. 发送请求
  - 6. 获取数据
  - 7. 关闭资源
  
#### 4. HttpClient请求参数配置

- 流程
  - 0. 创建请求配置对象
  - 1. 创建HttpClient对象
  - 2. 创建HttpPost/HttpGet对象
  - 3. 发起请求
  - 4. 判断响应状态码并获取数据
  - 5. 关闭资源
  
- 代理服务器的使用
  [代理服务器汇总网址](https://proxy.mimvp.com/freeopen)
  
#### 5. HttpClients进行封装

- UserAgent: 用户代理（User Agent，简称 UA），是一个特殊字符串头，使得服务器能够识别客户使用的操作系统及版本、CPU 类型、浏览器及版本、浏览器渲染引擎、浏览器语言、浏览器插件

### jsoup的使用

#### 1. 介绍

- jsoup 是一款基于 Java 语言的 HTML 请求及解析器，可直接请求某个 URL 地址、解析 HTML 文本内容。它提供了一套非常省力的 API，可通过 DOM、CSS 以及类似于 jQuery 的操作方法来取出和操作数据。
jsoup的主要功能如下：
  - 1.从一个URL，文件或字符串中解析HTML；
  - 2.使用DOM或CSS选择器来查找、取出数据；
  - 3.可操作HTML元素、属性、文本；
  
- 注意：jsoup对多线程、线程池支持不太好，因此仅将其作为解析Html工具

- jsoup 中的 Node、Element、Document

#### 2.解析html

- 使用dom方式遍历文档
- 元素中数据获
- 使用选择器语法查找元素
  - jsoup elements对象支持类似于CSS (或jquery)的选择器语法，来实现非常强大和灵活的查找功能。
  - select方法在Document/Element/Elements对象中都可以使用。可实现指定元素的过滤，或者链式选择访问。
- 任意组合，比如：span[abc].s_name
- ancestor child: 查找某个元素下子元素，比如：.city_con li 查找"city_con"下的所有li，
- parent > child: 查找某个父元素下的直接子元素， 比如：.city_con > ul > li 查找city_con第一级（直接子元素）的ul，再找所有ul下的第一级li
- parent > *查找某个父元素下所有直接子元素.city_con >*

### 补充知识点

#### 1. 正则表达式

[菜鸟教程](https://www.runoob.com/regexp/regexp-syntax.html)
[其他资料](https://deerchao.cn/tutorials/regex/regex.html)

#### 2. HTTP状态码

#### 3. HTTP请求头

> ## 疫情数据爬取

- 环境配置
  - pom.xml
  - application.properties
  
- 创建HttpUtils和TimeUtils工具类

- 创建爬取数据程序包与生成数据工具包

- 构造CovidBean用于Covid19DataCrawler读取数据

- 使用kafka
  - 在pom.xml文件中导入依赖
  - 新建config包-创建kafka配置类
  
- 将数据发送给Kafka

- 完成定时发送数据到Kafka
  - 创建ScheduleTest.class演示JDK自带的定时任务API，但不太方便使用
  - 演示SpringBoot整合好的定时任务

> ## 疫情数据生成并发送至kafka

- 注意KafkaTemplate设置发送到Kafka的Key/Value值需要注意类型

```java
 // 设置发送到kafka中的消息的Key/Value序列化类型，指定为<LocationId:Integer,Value:String>
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
```

- 注意Kafka使用时，注意防火墙状态

- 使用Kafka时注意ip与主机名称的映射，如果主机名无法解析则应更改配置文件为ip地址

# 三、数据的实时处理

>  ## 环境准备

- 在Covid19工程目录下新建名为 ```process``` 的Module(Maven工程)
- 配置pom.xml文件
- 在src文件夹下新建```scala```文件夹,在文件夹下新建```cn.covid```包,同时新建```bean process util```包

> ## 物资数据实时处理

- [IDEA配置sacla支持](https://blog.csdn.net/wmpisadba/article/details/119240546)

- 使用SparkStreaming实现
  - 准备SparkStreaming的开发环境
  - 准备Kafka的连接参数
  - 连接kafka获取消息
  - 实时处理数据
  - 将处理分析的结果存入到MySql
  - 开启SparkStreaming任务并等待结束

- SpaikStreaming整合Kafka的两种方式
  - Receiver 模式
    - 通过KafkaUtils.creatDStream -- API创建
    - 会有一个Receiver作为常驻Task，运行在Executor进程中，一直等待数据到来
    - 一个Reciver效率会比较低，可以使用多个Receiver,但是多个Receiver中的数据有需要手动进行合并，很麻烦。同时如果其中某个Receiver挂了，会导致数据丢失，需要开启WAL预写日志来保证数据安全，但效率低
    - Receiver模式使用Zookeeper模式来连接Kafka(Kafka的最新版本中已经不推荐使用该方法了)
    - Receiver模式中使用的是Kafka的高阶API，offset由Receiver提交到ZK中(Kafka的新版本中Offset默认存储在默认主题__consumer__offset中的，不推荐存入ZK中)，容易和Spark维护在Checkpoint中的offset不一致
  - Direct模式
    - 由KafkaUtils.createDirectStream--API创建
    - Direct模式是直接连接到Kafka的各个分区，并拉取得数据，提高了数据读取的并发能力
    - Direct模式使用的是kafka低阶API，可以自己维护偏移量到任何地方
    - 默认是由Spark提交到默认主题/CheckPoint
    - Direct模式+手动操作可以保证数据的Exactly-Once精准一次(数据仅会被处理一次)

- SparkStreaming整合 Kafka的两个版本API
  - SparkStreaming-kafka-0-8：支持Receiver模式和Direct模式，但是不支持offset维护API，不支持动态分区订阅
  - SparkStreaming-kafka-0-10：不支持Receiver模式，支持Direct模式，offset维护API，支持动态分区订阅

- 整合Kafka手动维护偏移量
  - 手动提交偏移量，就意味着消费了一批数据就应该提交一次偏移量，在SparkStreaming中数据抽象为D Stream，DStream底层其实也就是RDD，即每一批次的数据，因此需要对DStream中的RDD进行处理
  
  ```scala
  kafkaDs.foreachRDD(rdd=>{
      if(rdd.count() > 0){  //如果rdd中有数据则进行处理
        rdd.foreach(record => println("从kafka中消费到的每一条消息：" + record))
        //从kafka中消费到的每一条消息：ConsumerRecord(topic = covid19_material, partition = 0, leaderEpoch = 10, offset = 29, CreateTime = 1654518171097, serialized key size = -1, serialized value size = 7, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = spark)
        // 获取偏移量，使用Sprak-streaming-kafka-0-10中封装好的API来存放偏移量并提交
        val offsets: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for(o <- offsets) {
          println(s"topics = ${o.topic},partition = ${o.partition},util=${o.untilOffset}")
          //topics = covid19_material,partition = 0,util=30
        }
        // 手动提交偏移量到kafka的默认主题：__consumer__offsets中，如果开启了Checkpoint还会提交到Checkpoint中
        kafkaDs.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
      }
    })
  ```

- 创建 OffsetUtils 整合Kafka手动提交偏移量2
  - 创建OffsetsUtils工具类，将偏移量存储到MySQL中
  - 存入MySQL的偏移量将在```第3步连接kafka获取消息```之前取出并用于连接Kafka
  
  ```scala
   kafkaDs.foreachRDD(rdd=>{
        if(rdd.count() > 0){  //如果rdd中有数据则进行处理
          rdd.foreach(record => println("从kafka中消费到的每一条消息：" + record))
          //从kafka中消费到的每一条消息：ConsumerRecord(topic = covid19_material, partition = 0, leaderEpoch = 10, offset = 29, CreateTime = 1654518171097, serialized key size = -1, serialized value size = 7, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = spark)
          // 获取偏移量，使用Sprak-streaming-kafka-0-10中封装好的API来存放偏移量并提交
          val offsets: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          for(o <- offsets) {
            println(s"topics = ${o.topic},partition = ${o.partition},formOffset = ${o.fromOffset},util = ${o.untilOffset}")
            //topics = covid19_material,partition = 0,util=30
          }
          // 手动提交偏移量到kafka的默认主题：__consumer__offsets中，如果开启了Checkpoint还会提交到Checkpoint中
          // kafkaDs.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
          OffsetUtils.saveOffsets("SparkKafka",offsets)
        }
      })
  ```

- 数据实时分析与处理
  
- 数据聚合

- 聚合结果存入MySql
  - 创建工具类 BaseJdbcSink
  - 分别将数据聚合结果存入MySQL

# 四、数据的实时展示

> ## 技术栈

- Spring Boot
- Echarts
- Mybatis
- Lombok

> ## 环境准备
  
- 新建SpringBoot工程
- 拷贝静态资源到static目录下
- 新建controller，bean，mapper包

> ## Echarts入门

- ECharts开源来自百度商业前端数据可视化团队，基于html5 Canvas，是一个纯Javascript图表库，提供直观，生动，可交互，可个性化定制的数据可视化图表。创新的拖拽重计算、数据视图、值域漫游等特性大大增强了用户体验，赋予了用户对数据进行挖掘、整合的能力。
- 链接
[Echarts官网](https://echarts.apache.org/zh/index.html)
[菜鸟教程](https://www.runoob.com/w3cnote/html5-canvas-eccharts.html)
[入门案例](https://www.runoob.com/echarts/echarts-tutorial.html)

### 异步数据加载