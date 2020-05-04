package s3mafkaclient

import (
	"fmt"
	"s3common/s3castleclient"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

type s3SenderCallback struct{}
type s3MessageHandler struct{}

func (handler *s3SenderCallback) OnSuccess(msgs []interface{}) {
	fmt.Println("callback: messages send success!")
	for _, msg := range msgs {
		m, _ := msg.(string)
		fmt.Printf("callback: success message=[%s]\n", m)
	}

}

func (handler *s3SenderCallback) OnFailure(msgs []interface{}, err error) {
	fmt.Printf("callback: message send fail, err=[%s]\n", err)
	for _, msg := range msgs {
		m, _ := msg.(string)
		fmt.Printf("callback: fail send message=[%s]\n", m)
	}
}

func (s3MessageHandler) RecvMessage(msg *ConsumerMessage) int {
	count++
	fmt.Printf("Handler: recv messages from cluster=[%s] topic=[%s] partion=[%d] offset=[%d] key=[%v] value=[%v]\n", msg.Cluster, msg.Topic, msg.Partition, msg.Offset, msg.Key, string(msg.Value))
	return ConsumeStatusSuccess
}

var (
	testTopic = "crossreplicator_oa_smallobject" // hanmz-test-go

	//this testgroupID is used for test case.
	testGroupID            = "test-local-integration"
	testHost               = "172.18.170.156"
	testAppKey             = "com.sankuai.cloudoffice.mssappbj.crossreplic" //com.sankuai.inf.mafka.hanmz
	testClientIp           = "172.18.170.156"
	testCastleServerAppKey = "com.sankuai.inf.mafka.castlecommon"
	testCastleServerName   = "com.meituan.mafka.thrift.castle.CastleService"

	testcandidateBrokerNum       = 3
	testBufferSize         int64 = 10

	testManager      *s3castleclient.CastleClientManager
	testAsynProducer *MafkaAsynProducer
	testSynProducer  *MafkaSynProducer
	testConsumer     *MafkaConsumer

	messages = []string{"1111", "2222", "3333", "4444", "5555"}
	count    = 0
)

func iniCastle() (err error) {
	testManager, err = s3castleclient.NewCastleClientManager(testHost, testClientIp, testCastleServerAppKey, testCastleServerName, testAppKey)
	return
}

func iniProducer() (err error) {
	callback := &s3SenderCallback{}
	if testAsynProducer, err = NewMafkaAsynProducerWithCastleManager(testTopic, 4, 3, 500, testManager, nil, callback); err != nil {
		return
	}
	if testSynProducer, err = NewMafkaSynProducerWithCastleManager(testTopic, 3, testManager, nil); err != nil {
		return
	}
	return
}

func iniConsumer() (err error) {
	testConsumer, err = NewMafkaConsumerWithCastleManager(testTopic, testGroupID, testManager)
	return
}

func testSendMessages() (err error) {
	for _, msg := range messages {
		if err = testAsynProducer.SendMessageToChan("Asyn-" + msg); err != nil {
			return
		}
		if partition, offset, tmpErr := testSynProducer.SendMessage(msg + "-Syn"); tmpErr != nil {
			fmt.Printf("SynProducer send messasge fail, partion=[%d], offset=[%d], err=[%s]", partition, offset, tmpErr)
			return tmpErr
		}
	}

	var msgs []*sarama.ProducerMessage
	for _, item := range messages {
		if msg, temErr := testSynProducer.GenerateMessage(item); temErr != nil {
			fmt.Printf("SynProducer generate message fail, err=[%s]", temErr)
			continue
		} else {
			msgs = append(msgs, msg)
		}
	}
	err = testSynProducer.SendMessages(msgs)
	time.Sleep(time.Second * 10)
	return
}

func testConsumeMessages() {
	handler := &s3MessageHandler{}
	testConsumer.ConsumeMessage(handler)
	time.Sleep(time.Second * 20)
}

func TestMafkaClient(t *testing.T) {
	count = 0
	if err := iniCastle(); err != nil {
		t.Fatalf("iniCastle fail, err=[%s]", err)
	}

	if err := iniProducer(); err != nil {
		t.Fatalf("iniProducer fail, err=[%s]", err)
	}
	if err := testSendMessages(); err != nil {
		t.Fatalf("test sendMessage fail, err=[%s]", err)
	}
	testSynProducer.Close()
	testAsynProducer.Close()

	if err := iniConsumer(); err != nil {
		t.Fatalf("iniConsumer fail, err=[%s]", err)
	}

	testConsumeMessages()
	testConsumer.Close()
	testManager.Exit()

	t.Logf("cousumer received message nums=[%d]", count)
}
