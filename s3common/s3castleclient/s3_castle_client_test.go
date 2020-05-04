package s3castleclient

import (
	"testing"
	"time"

	"git.sankuai.com/inf/inf_go.git/go_mns_sdk/src/mns_sdk"
)

var (
	testSmallObjectTopic               = "abcdeef_oa_smallobject"
	testgroupID                        = "test-local-integration"
	testVersion                        = "2.2.18(java)"
	testHOST                           = "1.1.1.1"
	testAppKey                         = "com.xiaomi.cloudoffice.test.crossreplic"
	testClientIp                       = "1.1.1.1"
	testcandidateBrokerNum             = 5
	testManager                        *CastleClientManager
	testCastleUpdateIntevalSeconds     int64 = 20
	testClusterHeartBeatIntevalSeconds int64 = 5
	testThriftTimeoutSeconds           int64 = 2

	err                    error
	testOctoClient         *mns_sdk.MnsClient
	testcastleServerAppKey = "com.xiaomi.wps.gs.common"
	testcastleServerName   = "com.xiaomi.gs.tt.castle.cast"
)

func initTestManagerWithConfig() {
	testOctoClient = mns_sdk.MNSInit()
	testManager, _ = NewCastleClientManagerWithConfig(
		testCastleUpdateIntevalSeconds,
		testClusterHeartBeatIntevalSeconds,
		testThriftTimeoutSeconds,
		testVersion,
		testHOST,
		testClientIp,
		testcandidateBrokerNum,
		testOctoClient,
		testcastleServerAppKey,
		testcastleServerName,
		testAppKey,
	)
}

func iniTestManager() (err error) {
	testManager, err = NewCastleClientManager(testHOST, testClientIp, testcastleServerAppKey, testcastleServerName, testAppKey)
	return
}

func TestUpdateCastle(t *testing.T) {
	if err = iniTestManager(); err != nil {
		t.Fatalf("init manager fail, err=%s", err)
	}

	if newServerList, err := testManager.getCastleServerList(); err != nil {
		t.Errorf("getCastleServerList fail, err = %s", err)
	} else {
		for _, nodeInfo := range newServerList {
			t.Log("caslte server list:")
			t.Log(nodeInfo.String())
		}
	}

	testManager.Exit()
	return
}

func TestUpdateBroker(t *testing.T) {
	if err = iniTestManager(); err != nil {
		t.Fatalf("init manager fail, err=%s", err)
	}

	if err = testManager.AddProducerTopic(""); err == nil {
		t.Error("add empty producer topic success")
	}
	if err = testManager.AddProducerTopic(testSmallObjectTopic); err != nil {
		t.Errorf("add producer topic fail, topic = %s, err = %s", testSmallObjectTopic, err)
	}

	if err = testManager.AddConsumerTopic(ConsumerInfo{}); err == nil {
		t.Error("add empty consumer topic success")
	}
	if err = testManager.AddConsumerTopic(ConsumerInfo{
		Topic:   testSmallObjectTopic,
		GroupID: testgroupID,
	}); err != nil {
		t.Errorf("add consumer topic fail, topic = %s, err = %s", testSmallObjectTopic, err)
	}

	time.Sleep(time.Duration(time.Second * 10))

	if addrsMap, err := testManager.GetProducerBrokerList(testSmallObjectTopic); err != nil || len(addrsMap) == 0 {
		t.Errorf("GetProducerBrokerList failed, err=[%s]", err)
	} else {
		t.Log("ProducerBroker Addrs")
		for cluster, addrs := range addrsMap {
			t.Logf("cluster=[%s], adds=[%s]\n", cluster, addrs)
		}
	}

	if addrsMap, err := testManager.GetConsumerBrokerList(testSmallObjectTopic, testgroupID); err != nil || len(addrsMap) == 0 {
		t.Errorf("GetConsumerBrokerList failed, err=[%s]", err)
	} else {
		t.Log("ConsumerBroker Addrs")
		for cluster, addrs := range addrsMap {
			t.Logf("cluster=[%s], adds=[%s]\n", cluster, addrs)
		}
	}

	testManager.Exit()
}
