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
	influx "github.com/influxdata/influxdb1-client/v2"
	ini "gopkg.in/ini.v1"
)

var subConnection mqtt.Client
var influxClient influx.Client
var cfg *ini.File

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
func MonitorMQTT() {
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
		subConnection = connect("MQTT2Influx-"+parser, uri)
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
		case "nilan":
			parseNilan(topic, payload)
		default:
		}
	})
}

// output from https://github.com/jascdk/Nilan_Homeassistant
func parseNilan(topic string, payload []byte) {
	r := regexp.MustCompile(`^[a-zA-Z0-9]*/(?P<group>[a-zA-Z0-9]*)/(?P<name>[a-zA-Z0-9_/]*)`)
	matches := r.FindStringSubmatch(topic)

	if len(matches) > 2 {
		tags := map[string]string{
			"group": matches[1],
			"name":  matches[2]}

		data := map[string]interface{}{
			"value": payload}

		point, _ := influx.NewPoint(
			"nilan",
			tags,
			data,
			time.Now(),
		)
		influxBatchPoint, _ := influx.NewBatchPoints(influx.BatchPointsConfig{
			Database:  cfg.Section("influxdb").Key("database").String(),
			Precision: "s",
		})
		influxBatchPoint.AddPoint(point)
		if err := influxClient.Write(influxBatchPoint); err != nil {
			log.Noticef("Error writing to influx: %s", err)
		}
	}
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

	data := map[string]interface{}{}

	if tags["type"] == "magnet" && tags["sensor"] == "status" {
		tags["raw"] = string(payload)
		if string(payload) == "close" {
			data["value"] = 1.0
		} else if string(payload) == "open" {
			data["value"] = 0.0
		} else {
			data["value"] = -1.0
		}
	} else if tags["type"] == "motion" && tags["sensor"] == "status" {
		tags["raw"] = string(payload)
		if string(payload) == "motion" {
			data["value"] = 1.0
		} else if string(payload) == "no_motion" {
			data["value"] = 0.0
		} else {
			data["value"] = -1.0
		}
	} else if tags["type"] == "sensor_switch.aq2" && tags["sensor"] == "status" {
		// FIXME
	} else if tags["type"] == "gateway" && tags["sensor"] == "rgb" {
		// FIXME
	} else {
		data["value"] = payload
	}

	point, _ := influx.NewPoint(
		"xiaomi",
		tags,
		data,
		time.Now(),
	)
	influxBatchPoint, _ := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  cfg.Section("influxdb").Key("database").String(),
		Precision: "s",
	})
	influxBatchPoint.AddPoint(point)
	if err := influxClient.Write(influxBatchPoint); err != nil {
		log.Noticef("Error writing to influx: %s", err)
	}

}

func parseSonoffPowR2(topic string, payload []byte) {
	r := regexp.MustCompile(`^(?P<prefix>[a-zA-Z0-9]*)/.*`)
	matches := r.FindStringSubmatch(topic)
	sensor := matches[1]

	var payloadMap map[string]interface{}
	json.Unmarshal(payload, &payloadMap)
	energyMap := payloadMap["ENERGY"].(map[string]interface{})

	tags := map[string]string{
		"name": sensor}

	data := map[string]interface{}{
		"total":         energyMap["Total"].(float64),
		"yesterday":     energyMap["Yesterday"].(float64),
		"today":         energyMap["Today"].(float64),
		"period":        energyMap["Period"].(float64),
		"power":         energyMap["Power"].(float64),
		"apparentpower": energyMap["ApparentPower"].(float64),
		"reactivepower": energyMap["ReactivePower"].(float64),
		"factor":        energyMap["Factor"].(float64),
		"voltage":       energyMap["Voltage"].(float64),
		"current":       energyMap["Current"].(float64)}

	for k, v := range data {
		log.Noticef("%s %s: %f\n", sensor, k, v)
	}

	point, _ := influx.NewPoint(
		"sonoffPowR2",
		tags,
		data,
		time.Now(),
	)
	influxBatchPoint, _ := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  cfg.Section("influxdb").Key("database").String(),
		Precision: "s",
	})
	influxBatchPoint.AddPoint(point)
	if err := influxClient.Write(influxBatchPoint); err != nil {
		log.Noticef("Error writing to influx: %s", err)
	}
}

func main() {
	var err error
	cfg, err = ini.Load(os.Args[1])

	if err != nil {
		log.Criticalf("Fail to read file: %v", err)
		os.Exit(1)
	}

	log.Infof("GoMQTT2Influx version: %s.\n", version.Version)

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

	log.Info("Connecting to influxDB")
	influxClient, _ = influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     cfg.Section("influxdb").Key("url").String(),
		Username: cfg.Section("influxdb").Key("username").String(),
		Password: cfg.Section("influxdb").Key("password").String(),
	})

	var wg sync.WaitGroup
	wg.Add(1)

	if mqttconfig {
		defer wg.Done()

		go MonitorMQTT()
	}

	if influxconfig {
	}

	wg.Wait()
}
