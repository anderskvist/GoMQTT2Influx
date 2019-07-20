package main

import (
	"fmt"
	"net/url"
	"os"
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

	if subConnection == nil {
		subConnection = connect("MQTT2Influx", uri)
		log.Debug("Connecting to MQTT (sub)")
	}

	subConnection.Subscribe("home/#", 0, func(client mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		payload := msg.Payload()

		log.Noticef("[%s] %s\n", topic, string(payload))
	})
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
