package s3castleclient

import (
	"context"
	"errors"
	"fmt"
	"s3common"
	"s3lib/s3thriftrpc/castle"
	"s3lib/s3thriftrpc/castleclient"
	log "s3lib/third/seelog"
	"sync"
	"time"

	"git.sankuai.com/inf/inf_go.git/go_mns_sdk/src/mns_sdk"
)

var (
	ErrCastleServerMapEmpty  = errors.New("castle server list is empty")
	ErrCastleResponseEmpty   = errors.New("castle response is empty")
	ErrCastleResponseErrCode = errors.New("castle response err code")
	ErrCastleServerAllFailed = errors.New("castle server all fail")
	ErrCastleInvalidConfig   = errors.New("castle invalid config")

	ErrCachedBrokerListEmpty     = errors.New("cached broker list is empty")
	ErrOctoGetServerListFailed   = errors.New("Octo get server list fail")
	ErrOctoNoAvailableServerList = errors.New("Octo response no available server")

	ErrProducerEmptyTopic = errors.New("empty producer topic")
	ErrConsumerEmptyTopic = errors.New("empty consumer topic")
	ErrObserverNotExisted = errors.New("observer is not existed")
)

const (
	NormalServerStatus int32 = 2
)

type consumerClusterInfoPair map[string]*castle.ConsumerClusterInfo
type producerClusterInfoPair map[string]*castle.ProducerClusterInfo

type Observer interface {
	UpdateConfig(addrsMap map[string][]string)
	Reinit(addrsMap map[string][]string)
	Info() string
	Exit()
}

type CastleServerInfo struct {
	Ip   string
	Port int32
}

type ConsumerInfo struct {
	Topic   string
	GroupID string
}

func (info *ConsumerInfo) String() string {
	return fmt.Sprintf("%s:%s", info.Topic, info.GroupID)
}

func (info *CastleServerInfo) String() string {
	return fmt.Sprintf("%s-%d", info.Ip, info.Port)
}

type CastleClientManager struct {
	mtOctoClient       *mns_sdk.MnsClient
	castleServerAppKey string
	castleServerName   string
	clientAppKey       string

	castleClientMap map[string]*castleclient.CastleClientEnableClose
	CastleMapLock   sync.RWMutex
	exitCh          chan int

	clusterInfoLock              sync.RWMutex
	cachedProducerClusterInfoMap map[string]producerClusterInfoPair
	cachedConsumerClusterInfoMap map[string]consumerClusterInfoPair

	topicLock         sync.RWMutex
	producerTopicList []string
	consumerTopicList []ConsumerInfo

	observerLock sync.RWMutex
	observerMap  map[string]Observer

	lastConsumerResponses map[string]*castle.HeartBeatResponse
	lastProducerResponses map[string]*castle.HeartBeatResponse

	version                        string
	hostname                       string
	ip                             string
	clientInstanceID               string
	thriftTimeoutSeconds           int64
	clusterHeartBeatIntevalSeconds int64
	castleUpdateIntevalSeconds     int64

	CandidateBrokerNum int
	context            context.Context
}

func NewCastleClientManager(
	hostName string,
	ip string,
	castleServerAppKey string,
	castleServerName string,
	clientAppKey string,
) (manager *CastleClientManager, err error) {
	if castleServerAppKey == "" || castleServerName == "" || clientAppKey == "" {
		return nil, ErrCastleInvalidConfig
	}
	var castleUpdateIntevalSeconds int64 = 5
	var clusterHeartBeatIntevalSeconds int64 = 5
	var thriftTimeoutSeconds int64 = 3
	var candidateBrokerNum int = 5
	var version = s3common.V0_1_1.String()
	var octoClient = mns_sdk.MNSInit()

	manager, err = NewCastleClientManagerWithConfig(castleUpdateIntevalSeconds, clusterHeartBeatIntevalSeconds,
		thriftTimeoutSeconds, version, hostName, ip, candidateBrokerNum, octoClient, castleServerAppKey, castleServerName, clientAppKey)
	return
}

func NewCastleClientManagerWithConfig(
	castleUpdateIntevalSeconds int64,
	clusterHeartBeatIntevalSeconds int64,
	thriftTimeoutSeconds int64,
	version string,
	hostName string,
	ip string,
	candidateBrokerNum int,
	octoClient *mns_sdk.MnsClient,
	castleServerAppKey string,
	castleServerName string,
	clientAppKey string,
) (manager *CastleClientManager, err error) {
	if castleUpdateIntevalSeconds <= 0 || clusterHeartBeatIntevalSeconds <= 0 || thriftTimeoutSeconds <= 0 ||
		candidateBrokerNum <= 0 || castleServerAppKey == "" || castleServerName == "" || clientAppKey == "" {
		return nil, ErrCastleInvalidConfig
	}

	manager = &CastleClientManager{
		castleClientMap:                make(map[string]*castleclient.CastleClientEnableClose),
		cachedProducerClusterInfoMap:   make(map[string]producerClusterInfoPair),
		cachedConsumerClusterInfoMap:   make(map[string]consumerClusterInfoPair),
		lastConsumerResponses:          make(map[string]*castle.HeartBeatResponse),
		lastProducerResponses:          make(map[string]*castle.HeartBeatResponse),
		observerMap:                    make(map[string]Observer),
		castleUpdateIntevalSeconds:     castleUpdateIntevalSeconds,
		clusterHeartBeatIntevalSeconds: clusterHeartBeatIntevalSeconds,
		thriftTimeoutSeconds:           thriftTimeoutSeconds,
		ip:                             ip,
		version:                        version,
		hostname:                       hostName,
		CandidateBrokerNum:             candidateBrokerNum,
		mtOctoClient:                   octoClient,
		castleServerAppKey:             castleServerAppKey,
		castleServerName:               castleServerName,
		clientAppKey:                   clientAppKey,
		exitCh:                         make(chan int),
	}
	manager.clientInstanceID = fmt.Sprintf("%d", s3common.GetIncTimeStamp())
	manager.context = context.Background()
	for i := 0; i < s3common.ConnectionRetryTimes; i++ {
		if err = manager.updateCastleServerList(); err != nil {
			time.Sleep(time.Millisecond * 200)
		} else {
			err = nil
			break
		}
	}
	if err != nil {
		return nil, err
	}
	go manager.updateCastleConnectionsLoop()
	go manager.syncBrokerListLoop()
	return
}

func (manager *CastleClientManager) getCastleServerList() (newServerList []CastleServerInfo, err error) {
	if successSign, srvList := manager.mtOctoClient.GetSvrList(manager.castleServerAppKey, manager.clientAppKey, "thrift", manager.castleServerName); successSign == false {
		log.Errorf("CaslteClient mtOctoClient get server list fail, castleServerAppKey=[%s] clientAppKey=[%s] castleServerName=[%s]", manager.castleServerAppKey, manager.clientAppKey, manager.castleServerName)
		err = ErrOctoGetServerListFailed
	} else {
		for _, serviceInfo := range srvList {
			if serviceInfo.Status != NormalServerStatus || serviceInfo.Weight <= 0 {
				continue
			} else {
				serverInfo := CastleServerInfo{
					Ip:   serviceInfo.Ip,
					Port: serviceInfo.Port,
				}
				newServerList = append(newServerList, serverInfo)
			}
		}
		if len(newServerList) == 0 {
			log.Warnf("CaslteClient no available octo server for use, castleServerAppKey=[%s] clientAppKey=[%s] castleServerName=[%s]", manager.castleServerAppKey, manager.clientAppKey, manager.castleServerName)
			err = ErrOctoNoAvailableServerList
		}
	}
	return
}

func (manager *CastleClientManager) AddProducerTopic(topic string) (err error) {
	if topic == "" {
		return ErrProducerEmptyTopic
	}
	manager.topicLock.Lock()
	has := false
	for _, t := range manager.producerTopicList {
		if t == topic {
			has = true
			break
		}
	}
	if !has {
		manager.producerTopicList = append(manager.producerTopicList, topic)
	}
	manager.topicLock.Unlock()
	return nil
}

func (manager *CastleClientManager) AddConsumerTopic(topicInfo ConsumerInfo) (err error) {
	if topicInfo.Topic == "" || topicInfo.GroupID == "" {
		return ErrConsumerEmptyTopic
	}
	manager.topicLock.Lock()
	has := false
	for _, t := range manager.consumerTopicList {
		if t.String() == topicInfo.String() {
			has = true
			break
		}
	}
	if !has {
		manager.consumerTopicList = append(manager.consumerTopicList, topicInfo)
	}
	manager.topicLock.Unlock()
	return nil
}

func (manager *CastleClientManager) updateCastleServerList() (err error) {
	if newServerList, temErr := manager.getCastleServerList(); temErr != nil {
		err = temErr
	} else if err = manager.updateCastleConnections(newServerList); err != nil {
		log.Warnf("CaslteClient update castle connections fail, err=[%s]", err)
	}
	return
}

func (manager *CastleClientManager) updateCastleConnections(newServerList []CastleServerInfo) (err error) {
	newNodeMap := make(map[string]bool)
	for _, nodeInfo := range newServerList {
		key := nodeInfo.String()
		newNodeMap[key] = true
	}

	manager.CastleMapLock.Lock()
	defer manager.CastleMapLock.Unlock()
	for key, node := range manager.castleClientMap {
		if _, ok := newNodeMap[key]; ok == false {
			node.Close()
			delete(manager.castleClientMap, key)
		}
	}

	for _, nodeInfo := range newServerList {
		key := nodeInfo.String()
		if _, ok := manager.castleClientMap[key]; ok == false {
			if newNode, temErr := castleclient.NewCastleRpcClient(
				nodeInfo.Ip,
				fmt.Sprintf("%d", nodeInfo.Port),
				manager.thriftTimeoutSeconds); temErr != nil {
				log.Errorf("CaslteClient new castle RpcClient fail, ip=[%s] port=[%s] err=[%s]", nodeInfo.Ip, nodeInfo.Port, temErr)
				err = temErr
				continue
			} else {
				manager.castleClientMap[key] = newNode
			}
		}
	}
	return
}

//和caslte保持同步心跳
func (manager *CastleClientManager) updateCastleConnectionsLoop() {
	log.Infof("CastleClient start UpdateCastleConnectionsLoop")
	ticker := time.NewTicker(time.Duration(time.Second * time.Duration(manager.castleUpdateIntevalSeconds)))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			manager.updateCastleServerList()
		case <-manager.exitCh:
			goto exit

		}
	}
exit:
	log.Infof("CastleClient UpdateCastleConnectionsLoop exit")
}

//更新相关的 broker 列表
func (manager *CastleClientManager) syncBrokerListLoop() {
	log.Infof("CastleClient start SyncBrokerList")
	//quick start
	manager.updateConsumerMafkaBrokerList()
	manager.updateProducerMafkaBrokerList()
	ticker := time.NewTicker(time.Duration(time.Second * time.Duration(manager.clusterHeartBeatIntevalSeconds)))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			manager.updateConsumerMafkaBrokerList()
			manager.updateProducerMafkaBrokerList()
		case <-manager.exitCh:
			goto exit

		}
	}
exit:
	log.Infof("CastleClient SyncBrokerList exit")
}

func (manager *CastleClientManager) updateConsumerMafkaBrokerList() (err error) {
	manager.topicLock.RLock()
	defer manager.topicLock.RUnlock()
	for _, consumerInfo := range manager.consumerTopicList {
		if err = manager.updateConsumerClusterInfo(consumerInfo); err != nil {
			log.Warnf("CastleClient UpdateConsumerClusterInfo fail, topicInfo=[%s] err=[%s]", consumerInfo.String(), err)
		}
	}
	return
}

func (manager *CastleClientManager) updateConsumerClusterInfo(consumerInfo ConsumerInfo) (err error) {
	request := castleclient.NewConsumerRequest(
		manager.version,
		manager.hostname,
		manager.ip,
		manager.clientAppKey,
		consumerInfo.Topic,
		consumerInfo.GroupID,
		manager.clientInstanceID,
	)
	var successSign bool = false
	manager.CastleMapLock.RLock()
	defer manager.CastleMapLock.RUnlock()

	if len(manager.castleClientMap) == 0 {
		err = ErrCastleServerMapEmpty
		return
	}

	for castleKey, castleClient := range manager.castleClientMap {
		if response, headBeatErr := castleClient.Client.GetHeartBeat(manager.context, request); headBeatErr != nil {
			log.Warnf("CastleClient get heart Beat fail, castleClientInfo=[%s] err=[%s]", castleKey, headBeatErr)
		} else if latestInfo, responseErr := manager.getConsumerInfo(response, consumerInfo.String()); responseErr != nil {
			log.Warnf("CastleClient get consumer info fail, castleClientInfo=[%s] err=[%s]", castleKey, responseErr)
		} else if latestInfo == nil {
			manager.clusterInfoLock.Lock()
			_, ok := manager.cachedConsumerClusterInfoMap[consumerInfo.String()]
			manager.clusterInfoLock.Unlock()
			successSign = true
			if ok {
				break
			}
		} else {
			log.Debugf("CastleClient get consumer info success")
			manager.clusterInfoLock.Lock()
			manager.cachedConsumerClusterInfoMap[consumerInfo.String()] = latestInfo
			manager.clusterInfoLock.Unlock()
			successSign = true
			break
		}
	}
	if successSign == false {
		log.Errorf("CastleClient try all castle server list, all fail")
		err = ErrCastleServerAllFailed
	}
	return
}

func (manager *CastleClientManager) getConsumerInfo(resp *castle.HeartBeatResponse, consumerInfo string) (info consumerClusterInfoPair, err error) {
	if resp.ErrorCode == castle.ErrorCode_OK {
		log.Debugf("CastleClient heart beat response consumer version=[%d]", resp.Version)
		if len(resp.ClientResponse.ConsumerResponse.ClusterInfoPair) == 0 {
			log.Warnf("CastleClient heart beat response ClusterInfoPair is empty")
			err = ErrCastleResponseEmpty
		} else {
			info = resp.ClientResponse.ConsumerResponse.ClusterInfoPair

			if lastConsumerResponse, ok := manager.lastConsumerResponses[consumerInfo]; ok {
				if lastConsumerResponse.Version < resp.Version {
					addrs := manager.generateConsumerAddrs(info)
					log.Warnf("CastleClient consumer response version changed, lastVersion=[%d]  newVersion=[%d]",
						lastConsumerResponse.Version, resp.Version)
					manager.notifyReinit(consumerInfo, addrs)
				}
			}
			manager.lastConsumerResponses[consumerInfo] = resp
		}
	} else if resp.ErrorCode == castle.ErrorCode_REGISTER_FAIL {
		if lastConsumerResponse, ok := manager.lastConsumerResponses[consumerInfo]; ok {
			if lastConsumerResponse.Version < resp.Version {
				log.Warnf("CastleClient consumer response REGISTER_FAIL, lastVersion=[%d]  newVersion=[%d]",
					lastConsumerResponse.Version, resp.Version)
				manager.notifyExit(consumerInfo)
			}
		}
		manager.lastConsumerResponses[consumerInfo] = resp
	} else if resp.ErrorCode == castle.ErrorCode_NO_CHANGE {
		log.Infof("CastleClient HeartBeatResponse ConsumerClusterInfo unchanged")
	} else if resp.ErrorCode == castle.ErrorCode_NO_PARTITION_ASSIGN {
		log.Warnf("CastleClient HeartBeatResponse no partition assign to this comsumer")
	} else {
		log.Errorf("CastleClient HeartBeatResponse error, errCode=[%d]", resp.ErrorCode)
		err = ErrCastleResponseErrCode
	}
	return
}

func (manager *CastleClientManager) updateProducerMafkaBrokerList() (err error) {
	manager.topicLock.RLock()
	defer manager.topicLock.RUnlock()
	for _, producerTopic := range manager.producerTopicList {
		if err = manager.updateProducerClusterInfo(producerTopic); err != nil {
			log.Warnf("CastleClient UpdateProducerClusterInfo fail, topic=[%s] err=[%s]", producerTopic, err)
		}
	}
	return
}

func (manager *CastleClientManager) updateProducerClusterInfo(topic string) (err error) {
	request := castleclient.NewProducerRequest(
		manager.version,
		manager.hostname,
		manager.ip,
		manager.clientAppKey,
		topic,
		manager.clientInstanceID,
	)
	var successSign bool = false

	manager.CastleMapLock.RLock()
	defer manager.CastleMapLock.RUnlock()

	if len(manager.castleClientMap) == 0 {
		err = ErrCastleServerMapEmpty
		return
	}

	for castleKey, castleClient := range manager.castleClientMap {
		if response, headBeatErr := castleClient.Client.GetHeartBeat(manager.context, request); headBeatErr != nil {
			log.Warnf("CastleClient GetHeartBeat fail, castleClientInfo=[%s] err=[%s]", castleKey, headBeatErr)
		} else if latestInfo, responseErr := manager.getProducerInfo(response, topic); responseErr != nil {
			log.Warnf("CastleClient getProducerInfo fail, castleClientHost=[%s] err=[%s] version=[%s] hostname=[%s] ip=[%s] clientAppKey=[%s] topic=[%s] clientInstanceID=[%s] castleKey=[%s]",
				castleKey,
				responseErr,
				manager.version,
				manager.hostname,
				manager.ip,
				manager.clientAppKey,
				topic,
				manager.clientInstanceID,
				castleKey)
		} else {
			if latestInfo != nil {
				log.Debugf("CastleClient getProducerInfo success, version=[%s] hostname=[%s] ip=[%s] clientAppKey=[%s] topic=[%s] clientInstanceID=[%s] castleKey=[%s]", manager.version,
					manager.hostname,
					manager.ip,
					manager.clientAppKey,
					topic,
					manager.clientInstanceID,
					castleKey)
				manager.clusterInfoLock.Lock()
				manager.cachedProducerClusterInfoMap[topic] = latestInfo
				manager.clusterInfoLock.Unlock()
			}
			successSign = true
			break
		}
	}
	if successSign == false {
		log.Errorf("CastleClient try all castle server list, all failed")
		err = ErrCastleServerAllFailed
	}
	return
}

func (manager *CastleClientManager) getProducerInfo(resp *castle.HeartBeatResponse, producerInfo string) (info producerClusterInfoPair, err error) {
	if resp.ErrorCode == castle.ErrorCode_OK {
		log.Debugf("CastleClient HeartBeatResponse producer version=[%d]", resp.Version)
		if len(resp.ClientResponse.ProducerResponse.ClusterInfoPair) == 0 {
			log.Warnf("CastleClient HeartBeatResponse ClusterInfoPair is empty")
			err = ErrCastleResponseEmpty
		} else {
			info = resp.ClientResponse.ProducerResponse.ClusterInfoPair

			if lastProducerResponse, ok := manager.lastProducerResponses[producerInfo]; ok {
				if lastProducerResponse.Version < resp.Version {
					addrs := manager.generateProducerAddrs(info)
					log.Warnf("CastleClient producer response version changed, lastVersion=[%d]  newVersion=[%d]",
						lastProducerResponse.Version, resp.Version)
					manager.notifyReinit(producerInfo, addrs)
				}
			}
			manager.lastProducerResponses[producerInfo] = resp

		}
	} else if resp.ErrorCode == castle.ErrorCode_REGISTER_FAIL {
		if lastProducerResponse, ok := manager.lastProducerResponses[producerInfo]; ok {
			if lastProducerResponse.Version < resp.Version {
				log.Warnf("CastleClient producer response REGISTER_FAIL, lastVersion=[%d]  newVersion=[%d]",
					lastProducerResponse.Version, resp.Version)
				manager.notifyExit(producerInfo)
			}
		}
		manager.lastProducerResponses[producerInfo] = resp
	} else if resp.ErrorCode == castle.ErrorCode_NO_CHANGE {
		log.Infof("CastleClient HeartBeatResponse ProducerClusterInfo unchanged")
	} else {
		log.Errorf("CastleClient HeartBeatResponse error, errCode=[%d]", resp.ErrorCode)
		err = ErrCastleResponseErrCode
	}
	return
}

func (manager *CastleClientManager) Exit() {
	close(manager.exitCh)
	manager.CastleMapLock.RLock()
	defer manager.CastleMapLock.RUnlock()
	for clientAdd, castleClient := range manager.castleClientMap {
		castleClient.Close()
		log.Infof("CastleClient close RPC client, castleClient=[%s]", clientAdd)
	}
}

func (manager *CastleClientManager) GetProducerBrokerList(topic string) (addrsMap map[string][]string, err error) {
	manager.clusterInfoLock.RLock()
	clusterInfo, exit := manager.cachedProducerClusterInfoMap[topic]
	manager.clusterInfoLock.RUnlock()
	if exit == false {
		log.Warnf("CastleClient no related clusterInfo, topic=[%s]", topic)
		err = ErrCachedBrokerListEmpty
	} else if len(clusterInfo) == 0 {
		log.Warnf("CastleClient no related clusterInfo, topic=[%s]", topic)
		err = ErrCachedBrokerListEmpty
	} else {
		addrsMap = manager.generateProducerAddrs(clusterInfo)
	}
	return
}

func (manager *CastleClientManager) GetConsumerBrokerList(topic string, groupID string) (addrsMap map[string][]string, err error) {
	topicInfo := ConsumerInfo{
		Topic:   topic,
		GroupID: groupID,
	}
	manager.clusterInfoLock.RLock()
	clusterInfo, exit := manager.cachedConsumerClusterInfoMap[topicInfo.String()]
	manager.clusterInfoLock.RUnlock()

	if exit == false {
		log.Warnf("CastleClient no related clusterInfo, topic=[%s] groupID=[%s]", topic, groupID)
		err = ErrCachedBrokerListEmpty
	} else if len(clusterInfo) == 0 {
		log.Warnf("CastleClient no related clusterInfo, topic=[%s] groupID=[%s]", topic, groupID)
		err = ErrCachedBrokerListEmpty
	} else {
		addrsMap = manager.generateConsumerAddrs(clusterInfo)
	}
	return
}

func (manager *CastleClientManager) generateConsumerAddrs(clusterInfoPair consumerClusterInfoPair) (addrsMap map[string][]string) {
	addrsMap = make(map[string][]string)
	for cluster, info := range clusterInfoPair {
		count := 0
		var addrs []string
		for _, brokerInfo := range info.BrokerInfos {
			if count == manager.CandidateBrokerNum {
				break
			} else {
				addrs = append(addrs, fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port))
				count++
			}
		}
		addrsMap[cluster] = addrs
	}
	return
}

func (manager *CastleClientManager) generateProducerAddrs(clusterInfoPair producerClusterInfoPair) (addrsMap map[string][]string) {
	addrsMap = make(map[string][]string)
	for cluster, info := range clusterInfoPair {
		var addrs []string
		count := 0
		for _, brokerInfo := range info.BrokerInfos {
			if count == manager.CandidateBrokerNum {
				break
			} else {
				addrs = append(addrs, fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port))
				count++
			}
		}
		addrsMap[cluster] = addrs
	}
	return
}

func (manager *CastleClientManager) RegisterObserver(observer Observer) string {
	manager.observerLock.Lock()
	defer manager.observerLock.Unlock()
	clientID := fmt.Sprintf("%d", s3common.GetIncTimeStamp())
	manager.observerMap[clientID] = observer
	return clientID
}

func (manager *CastleClientManager) UnRegisterObserver(clientID string) error {
	manager.observerLock.Lock()
	defer manager.observerLock.Unlock()
	if _, ok := manager.observerMap[clientID]; ok {
		delete(manager.observerMap, clientID)
		log.Warnf("CastleClient remove observer, clientID=[%s]", clientID)
	} else {
		return ErrObserverNotExisted
	}
	return nil
}

func (manager *CastleClientManager) notifyReinit(info string, addrsMap map[string][]string) {
	manager.observerLock.RLock()
	defer manager.observerLock.RUnlock()
	for _, observer := range manager.observerMap {
		if observer != nil && observer.Info() == info {
			observer.Reinit(addrsMap)
		}
	}
}

func (manager *CastleClientManager) notifyExit(info string) {
	manager.observerLock.RLock()
	defer manager.observerLock.RUnlock()
	for _, observer := range manager.observerMap {
		if observer != nil && observer.Info() == info {
			observer.Exit()
		}
	}
}
