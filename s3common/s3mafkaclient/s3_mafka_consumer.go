package s3mafkaclient

import (
	"errors"
	"fmt"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/tsthght/s3folder/s3common"
	"github.com/tsthght/s3folder/s3common/s3castleclient"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

var (
	ErrConsumerInvalidConfig     = errors.New("invalid config")
	ErrConsumerClusterNotExisted = errors.New("cluster is not existed")
)

const (
	//返回该状态，自动提交offset
	ConsumeStatusSuccess = 0

	//该状态，不提交offset，需要手动提交offset
	ConsumeStatusFailure = 1
)

type MessageHandler interface {
	//接收到消息后主动调用
	RecvMessage(msg *ConsumerMessage) int
}

type MafkaConsumer struct {
	CastleManager *s3castleclient.CastleClientManager

	//支持多集群消费问题，每个集群开启一个消费者进行消费
	Consumers     map[string]*cluster.Consumer
	consumersLock sync.Mutex

	clientID string
	Config   *cluster.Config
	GroupID  string
	Topic    string

	reinitChan chan int
}

type ConsumerMessage struct {
	*sarama.ConsumerMessage
	Cluster string
}

func newConsumerMessage(message *sarama.ConsumerMessage, cluster string) *ConsumerMessage {
	msg := &ConsumerMessage{
		ConsumerMessage: message,
		Cluster:         cluster,
	}
	//为了兼容java客户端，前几个字节为标志信息
	msg.Value = msg.Value[13:]
	return msg
}

func NewMafkaConsumerWithCastleManager(topic string, groupID string, castleManager *s3castleclient.CastleClientManager) (client *MafkaConsumer, err error) {
	if topic == "" || groupID == "" || castleManager == nil {
		return nil, ErrConsumerInvalidConfig
	}

	mafkaConfig := cluster.NewConfig()
	mafkaConfig.Consumer.Return.Errors = true
	mafkaConfig.Group.Return.Notifications = true
	mafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	client = &MafkaConsumer{
		Topic:         topic,
		GroupID:       groupID,
		Config:        mafkaConfig,
		Consumers:     make(map[string]*cluster.Consumer),
		reinitChan:    make(chan int, 1),
		CastleManager: castleManager,
	}
	castleManager.AddConsumerTopic(s3castleclient.ConsumerInfo{
		Topic:   client.Topic,
		GroupID: client.GroupID,
	})
	//be careful, this is a sync inti api, which may halt for many seconds.
	if err = client.initLoop(castleManager); err != nil {
		return nil, err
	}
	client.clientID = castleManager.RegisterObserver(client)
	fmt.Printf("Consumer register, clientID=[%s]", client.clientID)
	return
}

func (c *MafkaConsumer) initLoop(castleManager *s3castleclient.CastleClientManager) (err error) {
	fmt.Printf("Consumer start initLoop")
	for i := 0; i < s3common.ConnectionRetryTimes; i++ {
		if brokersMap, temErr := castleManager.GetConsumerBrokerList(c.Topic, c.GroupID); temErr != nil || len(brokersMap) == 0 {
			err = temErr
			fmt.Printf("Consumer GetProducerBrokerList fail, err=[%v]", err)
			time.Sleep(time.Second * 2)
		} else if err = c.init(brokersMap); err != nil {
			fmt.Printf("Consumer initComsumer fail, err=[%s]", err)
		} else {
			err = nil
			fmt.Printf("Consumer initLoop success")
			break
		}
	}
	return
}

func (c *MafkaConsumer) init(brokersMap map[string][]string) (err error) {
	fmt.Printf("Consumer init, info=[%s]", brokersMap)

	flag := true
	for name, brokers := range brokersMap {
		if consumer, tmpErr := cluster.NewConsumer(brokers, c.GroupID, []string{c.Topic}, c.Config); tmpErr != nil {
			fmt.Printf("Consumer init ClusterConsumer fail, err=[%s]", tmpErr)
			err = tmpErr
			flag = false
			break
		} else {
			// consume errors
			go func() {
				for err := range consumer.Errors() {
					fmt.Printf("Consumer Errors, cluster=[%s] err=[%s]", name, err.Error())
				}
			}()
			// consume notifications
			go func() {
				for ntf := range consumer.Notifications() {
					fmt.Printf("Consumer rebalanced, cluster=[%s] notification=[%+v]", name, ntf)
				}
			}()
			c.Consumers[name] = consumer
		}
	}

	if flag {
		if len(c.reinitChan) == 0 {
			c.reinitChan <- 0
		}
	} else {
		for name, consumer := range c.Consumers {
			if consumer != nil {
				consumer.Close()
			}
			delete(c.Consumers, name)
		}
	}

	return
}

func (c *MafkaConsumer) ConsumeMessage(handler MessageHandler) {

	go func() {
		fmt.Printf("Consumer start consume message loop")

		for _ = range c.reinitChan {

			c.consumersLock.Lock()
			for k, v := range c.Consumers {
				cluster := k
				consumer := v
				go func() {
					for msg := range consumer.Messages() {
						if handler != nil {
							item := newConsumerMessage(msg, cluster)
							res := handler.RecvMessage(item)
							if res == ConsumeStatusSuccess {
								c.MarkOffset(item)
							}
						}
					}
				}()

			}
			c.consumersLock.Unlock()
		}

		fmt.Printf("Consumer exit consume message loop")
	}()

}

//支持手动提交Offset功能
func (c *MafkaConsumer) MarkOffset(msg *ConsumerMessage) (err error) {
	c.consumersLock.Lock()
	defer c.consumersLock.Unlock()
	if consumer, exist := c.Consumers[msg.Cluster]; exist {
		consumer.MarkPartitionOffset(msg.Topic, msg.Partition, msg.Offset, fmt.Sprintf("P:%d-O:%d", msg.Partition, msg.Offset))
	} else {
		err = ErrConsumerClusterNotExisted
	}
	return
}

//重置offset
func (c *MafkaConsumer) ResetOffset(msg *ConsumerMessage) (err error) {
	c.consumersLock.Lock()
	defer c.consumersLock.Unlock()
	if consumer, exist := c.Consumers[msg.Cluster]; exist {
		consumer.ResetOffset(msg.ConsumerMessage, fmt.Sprintf("P:%d-O:%d", msg.Partition, msg.Offset))
	} else {
		err = ErrConsumerClusterNotExisted
	}
	return
}

//立刻提交offset，一般不需要调用，程序会隔一段时间自动提交offset
func (c *MafkaConsumer) CommitOffset() (err error) {
	c.consumersLock.Lock()
	defer c.consumersLock.Unlock()
	for _, consumer := range c.Consumers {
		err = consumer.CommitOffsets()
	}
	return
}

func (c *MafkaConsumer) UpdateConfig(addrsMap map[string][]string) {
	c.consumersLock.Lock()
	defer c.consumersLock.Unlock()
	for cluster, addrs := range addrsMap {
		if consumer, exist := c.Consumers[cluster]; exist {
			consumer.RefreshConfig(addrs)
		}
	}
	fmt.Printf("Consumer updateConfig")
}

func (c *MafkaConsumer) Info() string {
	return fmt.Sprintf("%s:%s", c.Topic, c.GroupID)
}

//用户主动关闭客户端
func (c *MafkaConsumer) Close() {
	c.CastleManager.UnRegisterObserver(c.clientID)
	close(c.reinitChan)
	c.Exit()
	fmt.Printf("Consumer close")
}

//用于实现优雅重启功能
func (c *MafkaConsumer) Reinit(addrsMap map[string][]string) {
	fmt.Printf("Consumer start reinit")
	c.consumersLock.Lock()
	defer c.consumersLock.Unlock()
	c.cleanConsumers()
	err := c.init(addrsMap)
	if err != nil {
		fmt.Printf("Consumer reinit fail, err=[%s]", err)
	} else {
		fmt.Printf("Consumer reinit succ")
	}
}

//断开客户端连接，根据服务端返回状态，主动断开连接
func (c *MafkaConsumer) Exit() {
	c.consumersLock.Lock()
	c.cleanConsumers()
	c.consumersLock.Unlock()
	fmt.Printf("Consumer exit")
}

func (c *MafkaConsumer) cleanConsumers() {
	for cluster, consumer := range c.Consumers {
		if consumer != nil {
			consumer.Close()
		}
		delete(c.Consumers, cluster)
	}
}
