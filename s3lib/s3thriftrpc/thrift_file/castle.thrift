namespace go castle

struct HeartBeatRequest {
    1: i32 version, //初始上报为-1，以后上报的是当前版本号，服务端没有用
    2: ClientRole clientRole, //客户端类型
    3: i32 heartbeatTime, //客户端上报心跳时间，服务端没有用
    4: ClientInfo clientInfo, //客户端基本信息
    5: ClientConfig clientConfig, //要访问的队列信息
    6: Status status //客户端状态上报，ALIVE/DEAD
}
enum ClientRole {
    PRODUCER = 0,
    CONSUMER = 1,
    PUSH_CONSUMER = 2,
}
struct ClientInfo {
    1: string version, //客户端代码发布版本号,携带有客户端tag，如delay，swallow等等
    2: string hostname,
    3: string ip,
    4: optional string appkey = ""
}
struct ClientConfig {
    1: ProducerConfig producerConfig,
    2: ConsumerConfig consumerConfig,
    3: optional ClientCommonConfig clientCommonConfig
}
struct ProducerConfig {
    1: string appkey,
    2: string topic,
    3: string producerId
}
struct ConsumerConfig {
    1: string appkey,
    2: string topic,
    3: string groupName,
    4: string consumerId,
    5: string manualPartitions
}
struct ClientCommonConfig { //以下信息由mns-invoker提起
    1: optional string environment,
    2: optional string container,
    3: optional string cell
}
enum Status{
    ALIVE                           = 0,
    DEAD                            = 1 //关闭是主动上报caslte，caslte无需用超时判断
}




struct HeartBeatResponse {
    1: ErrorCode errorCode,
    2: i32 version, //当前的版本号，版本号变大说明内容又变更
    3: ClientResponse clientResponse
}
enum ErrorCode {
    OK                              = 0,    //request、response正常，且状态有变更
    NO_CHANGE                       = 1,    //request、response正常，状态未变更
    ILLEGAL_ROLE                    = 2,    //客户端角色设置非法
    ILLEGAL_APPKEY                  = 3,    //APPKEY不合法
    ILLEGAL_TOPIC                   = 4,    //TOPIC不合法
    ILLEGAL_GROUP                   = 5,    //消费组不合法
    REGISTER_FAIL                   = 6,    //客户端注册失败
    ILLEGAL_PARAM                   = 7,    //客户端hearbeat参数不合法
    ILLEGAL_CLIENT_INFO             = 8,    //客户端上报信息不合法
    ILLEGAL_CLIENT_CONFIG           = 9,    //客户端未配置
    ILLEGAL_PRODUCER_CONFIG         = 10,   //生产端未配置
    ILLEGAL_CONSUMER_CONFIG         = 11,   //消费端未配置
    NO_VERSION_FOUND                = 12,   //无法计算出当前heartbeat版本信息，可能原因：客户端配置信息缺失、broker缺失、partition缺失
    NO_TOPIC_CONFIG_FOUND           = 13,   //无法根据appkey、topic（group）找到对应的配置信息
    NO_BROKER_FOUND                 = 14,   //Broker全部宕机，或者网络闪断
    NO_CLUSTER_FOUND                = 15,   //当前客户端配置的集群不属于castle集群的管控中，castle集群在mcc配置管理的broker集群
    NO_PARTITION_FOUND              = 16,   //当前topic下没有发现有partition，这种情况是zk上信息被误删
    NO_PARTITION_ASSIGN             = 17,   //request、response正常, 原因：partition数<消费者数或者castle计算过慢，导致新上的消费者尚未进行分配
    ILLEGAL_CLIENT_TYPE             = 18,   //非法的客户端，在mafka的topic下启用swallow adaptor客户端或者在swallow adaptor的topic下使用mafka客户端
    OTHER_ERROR                     = 100
}
struct ClientResponse {
    1: ProducerResponse producerResponse,
    2: ConsumerResponse consumerResponse,
    3: optional ClientCommonResponse clientCommonResponse
}
struct ProducerResponse { //topic粒度
    1: map<string, string> kvPair, //producer在zk上的配置信息
    2: map<string, ProducerClusterInfo> clusterInfoPair //目前只会返回一个集群
}
struct ProducerClusterInfo {
    1: string clusterName,
    2: list<BrokerInfo> brokerInfos,
    3: list<i32> partitionList
}
struct BrokerInfo
{
    1: i32 id,
    2: string host,
    3: i32 port
}
struct ConsumerResponse { //group粒度
    1: map<string, string> kvPair, //consumer在zk上的配置信息
    2: map<string, ConsumerClusterInfo> clusterInfoPair, //跨集群消费问题
    3: list<PushServerInfo> pushServerInfo
}
struct ConsumerClusterInfo {
    1: string clusterName,
    2: list<BrokerInfo> brokerInfos,
    3: PartitionAssign partitionAssign
}
struct PartitionAssign {
    1: i32 generationId,
    2: list<i32> partitionList  //consumer分配到的partition
}
struct PushServerInfo {
    1: string id,
    2: optional string hostname,
    3: optional string ip,
    4: optional i32 port,
    5: optional string thriftAppkey
}
struct ClientCommonResponse { //生产者和消费者公共的回复信息
    1: map<string, string> kvPair
}


service Castle {
    HeartBeatResponse getHeartBeat(1:HeartBeatRequest request),
}