package s3mafkaclient

import (
	"bytes"
	"github.com/tsthght/s3folder/s3common"

	"github.com/Shopify/sarama"

	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/tsthght/s3folder/s3common/s3castleclient"
	"sync"
	"time"
	"fmt"
)

var (
	ErrPutMessageToChanFail  = errors.New("buffer channel is full")
	ErrProducerInvalidConfig = errors.New("invalid config")
	ErrProducerDisconnection = errors.New("producer is disconnected")
)

const (
	zero   byte = 0
	typeID byte = 23
)

type SenderCallback interface {
	//异步发送消息成功时，回调函数
	OnSuccess(msgs []interface{})

	//异步发送消息失败时，回调函数
	OnFailure(msgs []interface{}, err error)
}

type MafkaSynProducer struct {
	CastleManager *s3castleclient.CastleClientManager
	Producer      sarama.SyncProducer
	Config        *sarama.Config
	Topic         string
	RetryTimes    int
	clientID      string
	producerLock  sync.Mutex
}

func newSynProducer(topic string, retryTimes int, config *sarama.Config, castleManager *s3castleclient.CastleClientManager) (client *MafkaSynProducer) {
	client = &MafkaSynProducer{
		Topic:         topic,
		RetryTimes:    retryTimes,
		CastleManager: castleManager,
	}
	if config == nil {
		config := sarama.NewConfig()
		config.Producer.RequiredAcks = sarama.WaitForLocal
		fmt.Printf("Producer flush config=[%+v]", config.Producer.Flush)
		config.Producer.Return.Successes = true
	}
	client.Config = config
	return
}

func NewMafkaSynProducer(topic string, castleManager *s3castleclient.CastleClientManager, config *sarama.Config) (client *MafkaSynProducer, err error) {
	var retryTimes int = 3
	client, err = NewMafkaSynProducerWithCastleManager(topic, retryTimes, castleManager, config)
	return
}

func NewMafkaSynProducerWithCastleManager(topic string, retryTimes int, castleManager *s3castleclient.CastleClientManager,
	config *sarama.Config) (client *MafkaSynProducer, err error) {
	if topic == "" || retryTimes <= 0 || castleManager == nil {
		return nil, ErrProducerInvalidConfig
	}

	client = newSynProducer(topic, retryTimes, config, castleManager)
	castleManager.AddProducerTopic(client.Topic)
	for i := 0; i < s3common.ConnectionRetryTimes; i++ {
		if brokerAddrs, tmpErr := castleManager.GetProducerBrokerList(client.Topic); tmpErr != nil || len(brokerAddrs) == 0 {
			fmt.Printf("SynProducer GetProducerBrokerList fail, err=[%s]", tmpErr)
			err = tmpErr
			time.Sleep(time.Second * 2)
		} else if err = client.init(brokerAddrs); err != nil {
			fmt.Printf("SynProducer initSaramaProducer fail, err=[%+v]", err)
		} else {
			err = nil
			break
		}
	}
	if err != nil {
		return nil, err
	}
	client.clientID = castleManager.RegisterObserver(client)
	fmt.Printf("Producer register, clientID=[%s]", client.clientID)
	return
}

func (p *MafkaSynProducer) init(addrsMap map[string][]string) (err error) {
	fmt.Printf("Producer init, info=[%s]", addrsMap)

	// 只返回一个集群,取第一个即可
	for _, brokerAddrs := range addrsMap {
		if sp, temErr := sarama.NewSyncProducer(brokerAddrs, p.Config); temErr != nil {
			err = temErr
			fmt.Printf("Producer init fail, err=[%s]", err)
		} else {
			p.Producer = sp
		}
		break
	}
	return
}

func (p *MafkaSynProducer) GenerateMessage(item interface{}) (msg *sarama.ProducerMessage, err error) {
	msg = &sarama.ProducerMessage{}
	msg.Topic = p.Topic
	msg.Partition = int32(-1)
	if body, temErr := json.Marshal(item); temErr != nil {
		fmt.Printf("Producer marshal item fail, item=[%v] err=[%s]", item, temErr)
		err = temErr
	} else {
		var value []byte
		var size int32 = int32(len(body))
		buf := new(bytes.Buffer)

		if err := binary.Write(buf, binary.LittleEndian, size); err != nil {
			fmt.Printf("binary write fail, err=[%s]", err)
		} else {
			//为了兼容java客户端消息格式
			value = append(value, typeID)
			for i := 0; i < 8; i++ {
				value = append(value, zero)
			}
			value = append(value, buf.Bytes()...)
			value = append(value, body...)
			msg.Value = sarama.ByteEncoder(value)
		}

	}
	return
}

//发送单个消息
func (p *MafkaSynProducer) SendMessage(item interface{}) (partition int32, offset int64, err error) {
	if msg, temErr := p.GenerateMessage(item); temErr != nil {
		fmt.Printf("Producer generate message fail, item=[%v] err=[%s]", item, temErr)
		err = temErr
	} else {
		success := false
		curRetryTime := 0
		//防止客户端重启或断开连接
		p.producerLock.Lock()
		defer p.producerLock.Unlock()
		if p.Producer == nil {
			err = ErrProducerDisconnection
			return
		}
		for success == false && curRetryTime < p.RetryTimes {
			if temPartition, temOffset, temErr := p.Producer.SendMessage(msg); temErr != nil {
				err = temErr
				fmt.Printf("send message fail, it will retry, curRetryTime=[%d] err=[%s]", curRetryTime, err)
			} else {
				success = true
				partition = temPartition
				offset = temOffset
				break
			}
			curRetryTime++
		}
		if success == false {
			fmt.Printf("Producer send message fail, err=[%s]", err)
		} else {
			err = nil
			fmt.Printf("Producer send message succ, partition=[%d] offset=[%d]", partition, offset)
		}
	}
	return
}

//批量发送消息
func (p *MafkaSynProducer) SendMessages(msgs []*sarama.ProducerMessage) (err error) {
	if len(msgs) == 0 {
		return
	}

	success := false
	curRetryTime := 0
	p.producerLock.Lock()
	defer p.producerLock.Unlock()
	if p.Producer == nil {
		err = ErrProducerDisconnection
		return
	}
	for success == false && curRetryTime < p.RetryTimes {
		if temErr := p.Producer.SendMessages(msgs); temErr != nil {
			err = temErr
			fmt.Printf("send patch messages fail, it will retry, curRetryTime=[%d] err=[%s]", curRetryTime, err)
		} else {
			success = true
			break
		}
		curRetryTime++
	}

	if success == false {
		fmt.Printf("Producer send patch messages fail, nums=[%d] err=[%s]", len(msgs), err)
	} else {
		err = nil
		fmt.Printf("Producer send patch messages succ, nums=[%d]", len(msgs))
	}
	return
}

//提供用户主动关闭
func (p *MafkaSynProducer) Close() {
	p.CastleManager.UnRegisterObserver(p.clientID)
	p.Exit()
}

//配置动态更新
func (p *MafkaSynProducer) UpdateConfig(addrsMap map[string][]string) {
	fmt.Printf("Producer updateConfig")
	for _, addrs := range addrsMap {
		if p.Producer != nil {
			p.Producer.UpdateConfigAddrs(addrs)
		}
		break
	}
}

//客户端重启
func (p *MafkaSynProducer) Reinit(addrsMap map[string][]string) {
	fmt.Printf("Producer reinit")
	p.producerLock.Lock()
	defer p.producerLock.Unlock()
	if p.Producer != nil {
		p.Producer.Close()
		p.Producer = nil
	}
	err := p.init(addrsMap)
	if err != nil {
		fmt.Printf("Producer reinit fail, err=[%s]", err)
	} else {
		fmt.Printf("Producer reinit succ")
	}
}

//服务端主动要求断开连接
func (p *MafkaSynProducer) Exit() {
	p.producerLock.Lock()
	defer p.producerLock.Unlock()
	if p.Producer != nil {
		p.Producer.Close()
		p.Producer = nil
	}
	fmt.Printf("Producer exit")
}

func (p *MafkaSynProducer) Info() string {
	return p.Topic
}

type MafkaAsynProducer struct {
	*MafkaSynProducer

	AsyncBufferChan     chan interface{}
	AsyncBufferChanSize int64
	callback            SenderCallback

	messageBufferLock   sync.RWMutex
	messageBuffer       []interface{}
	patchCommitInterval time.Duration
}

func NewMafkaAsynProducer(
	topic string,
	castleManager *s3castleclient.CastleClientManager,
	config *sarama.Config, cb SenderCallback) (client *MafkaAsynProducer, err error) {
	var bufferSize int64 = 1024
	var retryTimes int = 3
	var patchCommitInterval int = 500
	client, err = NewMafkaAsynProducerWithCastleManager(topic, bufferSize, retryTimes, patchCommitInterval, castleManager, config, cb)
	return
}

func NewMafkaAsynProducerWithCastleManager(topic string,
	bufferSize int64,
	retryTimes int,
	patchCommitInterval int,
	castleManager *s3castleclient.CastleClientManager,
	config *sarama.Config, cb SenderCallback) (client *MafkaAsynProducer, err error) {
	if topic == "" || bufferSize <= 0 || retryTimes <= 0 || patchCommitInterval <= 0 || castleManager == nil {
		return nil, ErrProducerInvalidConfig
	}

	syncProducer := newSynProducer(topic, retryTimes, config, castleManager)
	client = &MafkaAsynProducer{
		MafkaSynProducer:    syncProducer,
		AsyncBufferChan:     make(chan interface{}, bufferSize),
		AsyncBufferChanSize: bufferSize,
		callback:            cb,
		patchCommitInterval: time.Millisecond * time.Duration(patchCommitInterval),
	}
	castleManager.AddProducerTopic(client.Topic)
	// be careful, this is a sync inti api, which may halt for many seconds.
	if err = client.initLoop(castleManager); err != nil {
		return nil, err
	}
	client.clientID = castleManager.RegisterObserver(client)
	fmt.Printf("Producer register, clientID=[%s]", client.clientID)
	return
}

func (p *MafkaAsynProducer) initLoop(castleManager *s3castleclient.CastleClientManager) (err error) {
	for i := 0; i < s3common.ConnectionRetryTimes; i++ {
		if brokerAddrs, temErr := castleManager.GetProducerBrokerList(p.Topic); temErr != nil || len(brokerAddrs) == 0 {
			err = temErr
			fmt.Printf("AsynProducer GetProducerBrokerList fail, err=[%v]", err)
			time.Sleep(time.Second * 2)
		} else if err = p.init(brokerAddrs); err != nil {
			fmt.Printf("AsynProducer initProducerAndStartHandler fail, err=[%+v]", err)
		} else {
			fmt.Printf("AsynProducer initLoop success")
			err = nil
			go p.handleProducer()
			break
		}
	}
	return
}

func (p *MafkaAsynProducer) handleProducer() {
	fmt.Printf("AsynProducer start HandleProducer")
	ticker := time.NewTicker(p.patchCommitInterval)
	defer ticker.Stop()
	for {
		select {
		case item, ok := <-p.AsyncBufferChan:
			if ok {
				p.messageBufferLock.Lock()
				p.messageBuffer = append(p.messageBuffer, item)
				if int64(len(p.messageBuffer)) >= p.AsyncBufferChanSize {
					if err := p.handleMessages(p.messageBuffer); err != nil {
						fmt.Printf("AsynProducer send messages fail, it will retry later, err=[%s]", err)
					} else {
						if p.callback != nil {
							p.callback.OnSuccess(p.messageBuffer)
						}
						p.messageBuffer = nil
					}
				}
				p.messageBufferLock.Unlock()
			} else {
				goto exit
			}
		case <-ticker.C:
			p.messageBufferLock.Lock()
			if len(p.messageBuffer) > 0 {
				if err := p.handleMessages(p.messageBuffer); err != nil {
					fmt.Printf("AsynProducer send messages fail, err=[%s]", err)
					if int64(len(p.messageBuffer)) > p.AsyncBufferChanSize || err == ErrProducerDisconnection {
						fmt.Printf("AsynProducer total failed message will be droped, count=[%d]", len(p.messageBuffer))
						if p.callback != nil {
							p.callback.OnFailure(p.messageBuffer, err)
						}
						p.messageBuffer = nil
					} else {
						fmt.Printf("AsynProducer will retry send messages later")
					}
				} else {
					if p.callback != nil {
						p.callback.OnSuccess(p.messageBuffer)
					}
					p.messageBuffer = nil
				}
			}
			p.messageBufferLock.Unlock()
		}
	}
exit:
	p.FlushMessages()
	fmt.Printf("AsynProducer exit HandleProducer")
}

//将buffer中的数据立刻发送出去
func (p *MafkaAsynProducer) FlushMessages() {
	p.messageBufferLock.Lock()
	if len(p.messageBuffer) > 0 {
		if err := p.handleMessages(p.messageBuffer); err != nil {
			if p.callback != nil {
				p.callback.OnFailure(p.messageBuffer, err)
			}
		} else {
			if p.callback != nil {
				p.callback.OnSuccess(p.messageBuffer)
			}
		}
		p.messageBuffer = nil
	}
	p.messageBufferLock.Unlock()
}

func (p *MafkaAsynProducer) handleMessages(items []interface{}) (err error) {
	if len(items) == 0 {
		return
	}
	var messages []*sarama.ProducerMessage
	for _, item := range items {
		if msg, temErr := p.GenerateMessage(item); temErr != nil {
			fmt.Printf("AsynProducer handle Messages, marshal item fail, item=[%v] err=[%s]", item, temErr)
			continue
		} else {
			messages = append(messages, msg)
		}
	}
	err = p.SendMessages(messages)
	return
}

//发送消息
func (p *MafkaAsynProducer) SendMessageToChan(msg interface{}) (err error) {
	if p.Producer == nil {
		return ErrProducerDisconnection
	}
	select {
	case p.AsyncBufferChan <- msg:
		fmt.Printf("AsynProducer put message to buffer chan success")
	default:
		fmt.Printf("AsynProducer put message to buffer chan fail, item=[%v]", msg)
		err = ErrPutMessageToChanFail
	}
	return err
}

func (p *MafkaAsynProducer) Close() {
	p.CastleManager.UnRegisterObserver(p.clientID)
	close(p.AsyncBufferChan)
	p.Exit()
	fmt.Printf("Producer close")
}

func (p *MafkaAsynProducer) Exit() {
	if p.Producer != nil {
		p.FlushMessages()
		p.producerLock.Lock()
		defer p.producerLock.Unlock()
		p.Producer.Close()
		p.Producer = nil
	}
	fmt.Printf("Producer exit")
}
