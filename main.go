package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/anderskvist/GoHelpers/log"
	"github.com/anderskvist/GoHelpers/version"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	ini "gopkg.in/ini.v1"
)

var subConnection mqtt.Client

func connect(clientID string, uri *url.URL) mqtt.Client {
	opts := createClientOptions(clientID, uri)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(3 * time.Second) {
	}
	if err := token.Error(); err != nil {
		log.Fatal(err)
	}
	return client
}

func createClientOptions(clientID string, uri *url.URL) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", uri.Host))
	opts.SetUsername(uri.User.Username())
	password, _ := uri.User.Password()
	opts.SetPassword(password)
	opts.SetClientID(clientID)
	opts.SetCleanSession(true)
	return opts
}

// MonitorMQTT will monitor MQTT for changes
func MonitorMQTT(cfg *ini.File) {
	mqttURL := cfg.Section("mqtt").Key("url").String()
	uri, err := url.Parse(mqttURL)
	if err != nil {
		log.Fatal(err)
	}

	topic := cfg.Section("mqtt").Key("topic").String()
	if topic == "" {
		log.Fatal("topic to subscribe to is missing in configuration")
	}

	topicsMap := make(map[string]byte)
	topics := strings.Split(topic, ",")
	for i := range topics {
		topicsMap[topics[i]+"/#"] = 0 // QoS
	}

	for t := range topicsMap {
		log.Debugf("Subscribing to %s", t)
	}

	parser := cfg.Section("mqtt").Key("parser").String()
	if parser == "" {
		log.Fatal("Parser missing in configuration")
	}

	if subConnection == nil {
		subConnection = connect("MQTT2Influx", uri)
		log.Debug("Connecting to MQTT (sub)")
	}

	subConnection.SubscribeMultiple(topicsMap, func(client mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		payload := msg.Payload()

		log.Debugf("[%s] %s\n", topic, string(payload))

		switch parser {
		case "xiaomi":
			parseXiaomi(topic, payload)
		case "sonoffPowR2":
			parseSonoffPowR2(topic, payload)
		default:
		}
	})
}

// output from aqara-mqtt (https://github.com/monster1025/aqara-mqtt)
func parseXiaomi(topic string, payload []byte) {
	r := regexp.MustCompile(`^(?P<prefix>[a-zA-Z0-9]*)/(?P<type>[a-zA-Z0-9_\.]*)/(?P<id>[a-zA-Z0-9]*)/(?P<sensor>[a-zA-Z0-9]*)`)
	matches := r.FindStringSubmatch(topic)

	tags := map[string]string{
		"type":   matches[2],
		"id":     matches[3],
		"sensor": matches[4]}

	log.Noticef("xiaomi type:%s id:%s sensor:%s - value:%s", tags["type"], tags["id"], tags["sensor"], payload)
}

func parseSonoffPowR2(topic string, payload []byte) {
	r := regexp.MustCompile(`^(?P<prefix>[a-zA-Z0-9]*)/.*`)
	matches := r.FindStringSubmatch(topic)
	sensor := matches[1]

	var payloadMap map[string]interface{}
	json.Unmarshal(payload, &payloadMap)
	energyMap := payloadMap["ENERGY"].(map[string]interface{})

	log.Noticef("%s voltage: %f\n", sensor, energyMap["Voltage"])
	log.Noticef("%s power: %f\n", sensor, energyMap["Power"])
	log.Noticef("%s current: %f\n", sensor, energyMap["Current"])
}

func main() {
	cfg, err := ini.Load(os.Args[1])

	if err != nil {
		log.Criticalf("Fail to read file: %v", err)
		os.Exit(1)
	}

	log.Infof("DVIEnergiSmartControl version: %s.\n", version.Version)

	influxconfig := false
	mqttconfig := false

	if cfg.Section("influxdb").Key("url").String() != "" {
		log.Info("Activating InfluxDB plugin")
		influxconfig = true
	}
	if cfg.Section("mqtt").Key("url").String() != "" {
		log.Info("Activating MQTT plugin")
		mqttconfig = true
	}

	var wg sync.WaitGroup
	wg.Add(1)

	if mqttconfig {
		defer wg.Done()

		go MonitorMQTT(cfg)
	}

	if influxconfig {
	}

	wg.Wait()
}
