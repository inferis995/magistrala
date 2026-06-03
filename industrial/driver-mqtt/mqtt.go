package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/absmach/magistrala/industrial/pkg/tags"
	"github.com/absmach/magistrala/pkg/messaging"
)

type Driver struct {
	gateway    tags.GatewayConfig
	tags       []tags.Tag
	publisher  messaging.Publisher
	stopChan   chan struct{}
	prevValues map[string]interface{}
	prevMu     sync.RWMutex
}

func NewDriver(gw tags.GatewayConfig, tagList []tags.Tag, pub messaging.Publisher) *Driver {
	return &Driver{
		gateway:    gw,
		tags:       tagList,
		publisher:  pub,
		stopChan:   make(chan struct{}),
		prevValues: make(map[string]interface{}),
	}
}

func (d *Driver) Start(ctx context.Context) error {
	broker := fmt.Sprintf("tcp://%s:%d", d.gateway.Host, d.gateway.Port)
	log.Printf("[mqtt-driver] bridging from %s", broker)
	// TODO: connect to external MQTT broker and subscribe to tag topics
	// Forward messages to Magistrala Publisher
	return nil
}

func (d *Driver) Stop() {
	close(d.stopChan)
}

func (d *Driver) OnMessage(topic string, payload []byte) {
	for _, tag := range d.tags {
		if strings.HasSuffix(topic, tag.Address) || tag.Address == "" {
			var val interface{}
			if tag.Subtopic != "" {
				val = extractJSONPath(payload, tag.Subtopic)
			} else {
				val = strings.TrimSpace(string(payload))
				if f, err := strconv.ParseFloat(val.(string), 64); err == nil {
					val = f
				}
			}

			if val == nil {
				continue
			}

			tv := tags.TagValue{
				TagID:     tag.ID,
				Name:      tag.Name,
				Value:     val,
				Timestamp: time.Now().UnixNano(),
				Quality:   "GOOD",
			}

			ctx := context.Background()
			msg := &messaging.Message{
				Channel:   d.gateway.ChannelID,
				Domain:    d.gateway.DomainID,
				Subtopic:  tv.TagID,
				Publisher: d.gateway.ID,
				Protocol:  "mqtt",
				Payload:   tv.ToPayload(),
				Created:   tv.Timestamp,
			}
			if err := d.publisher.Publish(ctx, d.gateway.ChannelID, msg); err != nil {
				log.Printf("[mqtt] publish error: %v", err)
			}
		}
	}
}

func extractJSONPath(data []byte, path string) interface{} {
	var obj map[string]interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil
	}
	parts := strings.Split(path, ".")
	var current interface{} = obj
	for _, p := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		current, ok = m[p]
		if !ok {
			return nil
		}
	}
	return current
}

func (d *Driver) HandleWrite(cmd tags.WriteCommand) error {
	return fmt.Errorf("mqtt write not yet implemented")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, _ := strconv.Atoi(v)
	return n
}

func RunMain() {
	log.Println("[mqtt-driver] starting...")

	gw := tags.GatewayConfig{
		ID:         getEnv("GATEWAY_ID", ""),
		Name:       getEnv("GATEWAY_NAME", "mqtt-gw"),
		DriverType: "mqtt",
		Host:       getEnv("BROKER_HOST", "localhost"),
		Port:       getEnvInt("BROKER_PORT", 1883),
		DomainID:   getEnv("DOMAIN_ID", ""),
		ChannelID:  getEnv("CHANNEL_ID", ""),
	}

	if gw.ID == "" || gw.ChannelID == "" {
		log.Fatal("GATEWAY_ID and CHANNEL_ID are required")
	}

	var tagList []tags.Tag
	if tagsJSON := getEnv("TAGS_JSON", ""); tagsJSON != "" {
		if err := json.Unmarshal([]byte(tagsJSON), &tagList); err != nil {
			log.Fatalf("invalid TAGS_JSON: %v", err)
		}
	}

	driver := NewDriver(gw, tagList, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := driver.Start(ctx); err != nil {
		log.Fatalf("mqtt driver start: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[mqtt-driver] shutting down...")
	driver.Stop()
}
