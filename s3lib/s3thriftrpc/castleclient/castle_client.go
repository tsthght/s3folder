package castleclient

import (
	"net"
	"s3lib/s3thriftrpc/castle"
	log "s3lib/third/seelog"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type CastleClientEnableClose struct {
	Client        *castle.CastleClient
	TransprotConn thrift.TTransport
}

func NewCastleRpcClient(serverHost string, port string, timeoutSecondes int64) (client *CastleClientEnableClose, err error) {
	if tSocket, socketErr := thrift.NewTSocketTimeout(net.JoinHostPort(serverHost, port), time.Second*time.Duration(timeoutSecondes)); socketErr != nil {
		err = socketErr
		log.Errorf("new thrift rpc socket failed, host=[%s] port=[%s] err=[%s]", serverHost, port, err)
	} else {
		transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
		if transport, transportErr := transportFactory.GetTransport(tSocket); transportErr != nil {
			err = transportErr
			log.Errorf("new thrift GetTransport err, host=[%s] port=[%s] err=[%s]", serverHost, port, err)
		} else {
			protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
			rawClient := castle.NewCastleClientFactory(transport, protocolFactory)
			if err = transport.Open(); err != nil {
				log.Errorf("open thrift rpc client err, host=[%s] port=[%s] err=[%s]", serverHost, port, err)
			} else {
				client = &CastleClientEnableClose{
					Client:        rawClient,
					TransprotConn: transport,
				}
			}
		}
	}
	return
}

func (client *CastleClientEnableClose) Close() {
	client.TransprotConn.Close()
}

func NewProducerRequest(
	version string,
	hostname string,
	ip string,
	appkey string,
	topic string,
	instanceID string) (heartbeatRequest *castle.HeartBeatRequest) {
	clientInfo := &castle.ClientInfo{
		Version:  version,
		Hostname: hostname,
		IP:       ip,
		Appkey:   appkey,
	}
	produceClientConfig := &castle.ProducerConfig{
		Appkey:     appkey,
		Topic:      topic,
		ProducerId: hostname + "-producer-" + instanceID + "-" + ip,
	}
	clientConfig := &castle.ClientConfig{
		ProducerConfig: produceClientConfig,
	}
	heartbeatRequest = &castle.HeartBeatRequest{
		Version:       -1,
		ClientRole:    castle.ClientRole_PRODUCER,
		HeartbeatTime: int32(time.Now().Unix()),
		ClientInfo:    clientInfo,
		ClientConfig:  clientConfig,
		Status:        castle.Status_ALIVE,
	}
	return
}

func NewConsumerRequest(
	version string,
	hostname string,
	ip string,
	appkey string,
	topic string,
	groupName string,
	instanceID string) (heartbeatRequest *castle.HeartBeatRequest) {
	clientInfo := &castle.ClientInfo{
		Version:  version,
		Hostname: hostname,
		IP:       ip,
		Appkey:   appkey,
	}
	consumerClientConfig := &castle.ConsumerConfig{
		Appkey:     appkey,
		Topic:      topic,
		GroupName:  groupName,
		ConsumerId: hostname + "-consumer-" + instanceID + "-" + ip,
	}
	clientConfig := &castle.ClientConfig{
		ConsumerConfig: consumerClientConfig,
	}
	heartbeatRequest = &castle.HeartBeatRequest{
		Version:       -1,
		ClientRole:    castle.ClientRole_CONSUMER,
		HeartbeatTime: int32(time.Now().Unix()),
		ClientInfo:    clientInfo,
		ClientConfig:  clientConfig,
		Status:        castle.Status_ALIVE,
	}
	return
}
