package es_collect

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

var PCSTAT_INDEX_NAME = "pc_stat"
var KEEP_INDEX_NUM = 5

var mapping = `
{
	"settings":{
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"cluster_name" : {
				"type" : "keyword"
			},
			"node_name" : {
				"type" : "keyword"
			},
			"index_name" : {
				"type" : "keyword"
			},
			"created" : {
				"type" : "date"
			}
		}
	}
}
`

// es client for get shards or indices and more
type Client struct {
	Ip       string
	Port     string
	User     string
	Password string
}

func CollectClient(ip string, port string, user string, password string) *Client {
	instance := new(Client)
	instance.Ip = ip
	instance.Port = port
	instance.User = user
	instance.Password = password
	return instance
}

func OutputClient(ip string, port string, user string, password string) elasticsearch.Client {
	url := "http://" + ip + ":" + port + "/"
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{url},
		Username:  user,
		Password:  password,
	})
	if err != nil {
		panic(err)
	}

	return *client

}

func GetShardMap(client Client) ShardMap {
	url := "http://" + client.Ip + ":" + client.Port + "/_cat/shards?h=state,index,shard,node,prirep"
	body, err := httpGetRequest(url, client.User, client.Password)
	if err != nil {
		log.Printf("get indices error,%v", err)
	}
	lines := splitWithoutNull(body, "\n")

	shardMap := ShardMap{}
	for _, shardStr := range lines {
		columns := splitWithoutNull(shardStr, " ")
		state := columns[0]
		if strings.Compare(state, "STARTED") != 0 {
			continue
		}
		indexName := columns[1]
		nodeName := columns[3]
		shardId := columns[2]
		prirep := columns[4]
		primary := false
		if prirep == "p" {
			primary = true
		}
		shard := Shard{indexName: indexName, nodeName: nodeName, shardId: shardId, primary: primary}
		shardKey := shard.getShardKey()
		shardMap[shardKey] = shard
		//log.Printf("%s , %s ,%s",columns[0], columns[1] ,columns[2])
	}
	return shardMap
}

func GetIndiceMap(client Client, indicesPrefix []string) IndexMap {
	url := "http://" + client.Ip + ":" + client.Port + "/_cat/indices?h=index,uuid"
	body, err := httpGetRequest(url, client.User, client.Password)
	if err != nil {
		log.Printf("get indices error,%v", err)
	}
	lines := splitWithoutNull(body, "\n")

	indexMap := IndexMap{}
	for _, indexStr := range lines {
		columns := splitWithoutNull(indexStr, " ")
		indexName := columns[0]
		uuid := columns[1]
		if checkInIndices(indexName, indicesPrefix) {
			indexMap[indexName] = Index{indexName: indexName, uuid: uuid}
		}
		//log.Printf("%s , %s",columns[0], columns[1])
	}
	return indexMap
}

func checkInIndices(index string, indicesPrefix []string) bool {
	for _, prefixStr := range indicesPrefix {
		if strings.HasPrefix(index, prefixStr) {
			return true
		}
	}
	return false
}

func FillShardMap(shardMap ShardMap, indexMap IndexMap) ShardMap {
	return FillShardMapFilterNode(shardMap, indexMap, "")
}

func FillShardMapFilterNode(shardMap ShardMap, indexMap IndexMap, nodeName string) ShardMap {
	for key, shard := range shardMap {
		index, exist := indexMap[shard.indexName]
		if (nodeName != "" && strings.Compare(shard.nodeName, nodeName) != 0) || !exist {
			if !exist {
				//log.Printf("skip fill Shard uuid,can't get index, shard:%s", shard.getShardKey())
			}
			delete(shardMap, shard.getShardKey())
			continue
		}
		shard.uuid = index.uuid
		shardMap[key] = shard
	}
	return shardMap
}

// split and skip empty String ""
func splitWithoutNull(s, sep string) []string {
	strs := strings.Split(s, sep)
	res := make([]string, 0)
	for _, str := range strs {
		if strings.Compare(str, "") != 0 {
			res = append(res, str)
		}
	}
	return res
}

func initPcstatIndex(esClient elasticsearch.Client, indexPrefix string) (string, error) {
	//create index if not exist
	realIndex := indexPrefix + "-" + time.Now().Format("2006_01_02")
	exists, err := esClient.Indices.Exists([]string{realIndex})
	if err != nil {
		fmt.Errorf("check index exists error,index_name: %s, %s", realIndex, err)
	}
	if exists.StatusCode == 404 {
		createIndex, err := esClient.Indices.Create(realIndex, esClient.Indices.Create.WithBody(strings.NewReader(mapping)))
		if err != nil || createIndex == nil || createIndex.StatusCode != 200 {
			return realIndex, fmt.Errorf("create index error error, index_name: %s, %s", realIndex, err)
		}
	}

	//detele index if exist
	dayBefore := time.Now().AddDate(0, 0, -KEEP_INDEX_NUM-1)
	toDeleteIndex := indexPrefix + "-" + dayBefore.Format("2006_01_02")
	deleteExist, _ := esClient.Indices.Exists([]string{toDeleteIndex})
	if deleteExist.StatusCode == 200 {
		deleteIndex, error := esClient.Indices.Delete([]string{toDeleteIndex})
		if error != nil || deleteIndex == nil || deleteIndex.StatusCode != 200 {
			fmt.Errorf("delete index error error, index_name: %s, %s", toDeleteIndex, error)
		}
	}
	return realIndex, nil
}

func PostPcstatData(client elasticsearch.Client, docs []PageCacheDoc) {
	esClient := client
	indexName, err := initPcstatIndex(esClient, PCSTAT_INDEX_NAME)
	if err != nil {
		fmt.Printf("create index error, skip bulk data, %s\n", err)
		return
	}

	bulkRequest, indexerError := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      indexName,
		Client:     &client,
		NumWorkers: 1,
	})
	if indexerError != nil {
		fmt.Errorf("create bulk indexer error, %s", indexerError)
	}
	for _, doc := range docs {
		json, _ := json.Marshal(doc)
		bulkRequest.Add(context.Background(), esutil.BulkIndexerItem{
			Action: "index",
			Index:  indexName,
			Body:   bytes.NewReader(json),
			// OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
			// 	fmt.Println("success")
			// },
			// OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
			// 	fmt.Println("failure")
			// },
		})
	}

	closeErr := bulkRequest.Close(context.Background())
	// if bulkResponse == nil {
	// 	fmt.Errorf("expected bulkResponse to be != nil; got nil")
	// 	return
	// }
	if closeErr != nil {
		fmt.Errorf("bulk es data error, %s", closeErr)
	}
	// if bulkResponse.Errors {
	// 	fmt.Errorf("bulk error")
	// 	for _, typeItem := range bulkResponse.Items {
	// 		for _, item := range typeItem {
	// 			fmt.Errorf(item.Error.Reason)
	// 		}
	// 	}
	// }
}

func httpGetRequest(url string, user string, password string) (string, error) {
	rep, err := http.NewRequest("GET", url, nil)
	// req.Header.Set("X-Custom-Header", "myvalue")
	if err != nil {
		fmt.Println("req err:")
		panic(err)
	}
	//设置用户密码和跳过tls
	rep.SetBasicAuth(user, password)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Timeout: 30 * time.Second, Transport: tr}
	//获取结果
	resp, err := client.Do(rep)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
		return "", err
	}
	return string(body), nil
}

type PageCacheDoc struct {
	Cache       map[string]int `json:"cache"`
	Primary     bool           `json:"primary"`
	ClusterName string         `json:"cluster_name"`
	NodeName    string         `json:"node_name"`
	IndexName   string         `json:"index_name"`
	Created     time.Time      `json:"created,omitempty"`
}
