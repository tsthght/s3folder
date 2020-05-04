package castleclient

import (
	"context"
	"net"
	"github.com/tsthght/s3folder/s3lib/s3thriftrpc/castle"
	"testing"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

var (
	HOST              = "1.1.1.1"
	PORT              = "2222"
	testIP            = ""
	testAppKey        = "abcdef"
	testClientVersion = "1.0.0(go)"
	testClientHost    = "localmac"
	testClientIp      = "6.6.6.6"
	testTopict        = "abcdef"
)

func generateHeadRequest() (heartbeatRequest *castle.HeartBeatRequest) {
	clientInfo := &castle.ClientInfo{
		Version:  testClientVersion,
		Hostname: testClientHost,
		IP:       testClientIp,
		Appkey:   testAppKey,
	}
	produceClientConfig := &castle.ProducerConfig{
		Appkey:     testAppKey,
		Topic:      testTopict,
		ProducerId: testClientHost + testClientIp,
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

func outputCastleResponse(response *castle.HeartBeatResponse, t *testing.T) {
	switch response.ErrorCode {
	case castle.ErrorCode_OK:
		for key, value := range response.ClientResponse.ProducerResponse.ClusterInfoPair {
			t.Logf("key=[%s]\n", key)
			for _, brokeInfo := range value.BrokerInfos {
				t.Logf("borkerhost=[%s]\n", brokeInfo.Host)
			}
		}
		break
	default:
		t.Errorf("response err=[%d]", response.ErrorCode)
	}
}

func TestCastleClient(t *testing.T) {
	tSocket, err := thrift.NewTSocket(net.JoinHostPort(HOST, PORT))
	if err != nil {
		t.Errorf("tSocket error:err=[%s]", err)
		return
	}
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	transport, err := transportFactory.GetTransport(tSocket)
	if err != nil {
		t.Errorf("GetTransport error:err=[%s]", err)
		return
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	client := castle.NewCastleClientFactory(transport, protocolFactory)
	if err := transport.Open(); err != nil {
		t.Errorf("Error opening:host=[%s] port=[%s] err=[%s]", HOST, PORT, err)
		return
	}
	defer transport.Close()

	castleResquest := generateHeadRequest()
	if castleResplse, err := client.GetHeartBeat(context.Background(), castleResquest); err != nil {
		t.Errorf("Error DoHeartBeatRequest: err=[%s]", err)
	} else {
		outputCastleResponse(castleResplse, t)
	}
}
