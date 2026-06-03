package opcua

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
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
	scanRate   time.Duration
	endpoint   string
}

func NewDriver(gw tags.GatewayConfig, tagList []tags.Tag, pub messaging.Publisher, scanRateMs int) *Driver {
	if scanRateMs <= 0 {
		scanRateMs = 1000
	}
	return &Driver{
		gateway:    gw,
		tags:       tagList,
		publisher:  pub,
		stopChan:   make(chan struct{}),
		prevValues: make(map[string]interface{}),
		scanRate:   time.Duration(scanRateMs) * time.Millisecond,
		endpoint:   fmt.Sprintf("opc.tcp://%s:%d", gw.Host, gw.Port),
	}
}

func (d *Driver) Start(ctx context.Context) error {
	log.Printf("[opcua] connecting to %s", d.endpoint)
	// TODO: implement with gopcua/opcua
	// For now, placeholder that logs readiness
	go d.scanLoop(ctx)
	return nil
}

func (d *Driver) Stop() {
	close(d.stopChan)
}

func (d *Driver) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(d.scanRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopChan:
			return
		case <-ticker.C:
			// TODO: implement OPC-UA read via subscription or polling
			log.Printf("[opcua] scan tick (placeholder)")
		}
	}
}

func (d *Driver) publishValues(values []tags.TagValue) {
	ctx := context.Background()
	for _, tv := range values {
		msg := &messaging.Message{
			Channel:   d.gateway.ChannelID,
			Domain:    d.gateway.DomainID,
			Subtopic:  tv.TagID,
			Publisher: d.gateway.ID,
			Protocol:  "opcua",
			Payload:   tv.ToPayload(),
			Created:   tv.Timestamp,
		}
		if err := d.publisher.Publish(ctx, d.gateway.ChannelID, msg); err != nil {
			log.Printf("[opcua] publish error: %v", err)
		}
	}
}

func (d *Driver) HandleWrite(cmd tags.WriteCommand) error {
	return fmt.Errorf("opcua write not yet implemented")
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
	log.Println("[opcua-driver] starting...")

	gw := tags.GatewayConfig{
		ID:         getEnv("GATEWAY_ID", ""),
		Name:       getEnv("GATEWAY_NAME", "opcua-gw"),
		DriverType: "opcua",
		Host:       getEnv("PLC_HOST", "localhost"),
		Port:       getEnvInt("PLC_PORT", 4840),
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

	scanRate := getEnvInt("SCAN_RATE_MS", 1000)
	driver := NewDriver(gw, tagList, nil, scanRate)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := driver.Start(ctx); err != nil {
		log.Fatalf("opcua driver start: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[opcua-driver] shutting down...")
	driver.Stop()
}
