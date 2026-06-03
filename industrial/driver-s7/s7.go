package s7

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
	gos7 "github.com/robinson/gos7"
)

type Driver struct {
	gateway    tags.GatewayConfig
	tags       []tags.Tag
	publisher  messaging.Publisher
	client     gos7.Client
	handler    *gos7.TCPClientHandler
	stopChan   chan struct{}
	prevValues map[string]interface{}
	prevMu     sync.RWMutex
	rack       int
	slot       int
	scanRate   time.Duration
}

func NewDriver(gw tags.GatewayConfig, tagList []tags.Tag, pub messaging.Publisher, rack, slot, scanRateMs int) *Driver {
	if scanRateMs <= 0 {
		scanRateMs = 1000
	}
	return &Driver{
		gateway:    gw,
		tags:       tagList,
		publisher:  pub,
		stopChan:   make(chan struct{}),
		prevValues: make(map[string]interface{}),
		rack:       rack,
		slot:       slot,
		scanRate:   time.Duration(scanRateMs) * time.Millisecond,
	}
}

func (d *Driver) Start(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", d.gateway.Host, d.gateway.Port)
	handler := gos7.NewTCPClientHandler(addr, d.rack, d.slot)
	handler.Timeout = 5 * time.Second

	if err := handler.Connect(); err != nil {
		return fmt.Errorf("s7 connect %s: %w", addr, err)
	}

	d.handler = handler
	d.client = gos7.NewClient(handler)
	log.Printf("[s7] connected to %s (rack=%d, slot=%d)", addr, d.rack, d.slot)

	go d.scanLoop(ctx)
	return nil
}

func (d *Driver) Stop() {
	close(d.stopChan)
	if d.handler != nil {
		d.handler.Close()
	}
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
			d.scan()
		}
	}
}

func (d *Driver) scan() {
	var values []tags.TagValue
	now := time.Now().UnixNano()

	for _, tag := range d.tags {
		dbNum, offset, size, bitOffset := parseS7Address(tag.Address)
		buf := make([]byte, size)

		if err := d.client.AGReadDB(dbNum, offset, size, buf); err != nil {
			continue
		}

		val := decodeS7Value(buf, tag.DataType, bitOffset)
		if val == nil {
			continue
		}

		d.prevMu.RLock()
		prev := d.prevValues[tag.ID]
		d.prevMu.RUnlock()

		if prev != nil && fmt.Sprintf("%v", prev) == fmt.Sprintf("%v", val) {
			continue
		}

		d.prevMu.Lock()
		d.prevValues[tag.ID] = val
		d.prevMu.Unlock()

		values = append(values, tags.TagValue{
			TagID:     tag.ID,
			Name:      tag.Name,
			Value:     val,
			Timestamp: now,
			Quality:   "GOOD",
		})
	}

	if len(values) > 0 {
		d.publishValues(values)
	}
}

func parseS7Address(address string) (dbNum, offset, size, bitOffset int) {
	// Formats: DB1.DBX0.0, DB1.DBD0, DB1.DBW0, M0.0, I0.0, Q0.0
	if !strings.HasPrefix(strings.ToUpper(address), "DB") {
		return 0, 0, 0, 0
	}

	parts := strings.SplitN(address, ".", 3)
	if len(parts) < 2 {
		return 0, 0, 0, 0
	}

	dbNum, _ = strconv.Atoi(parts[0][2:])
	spec := parts[1]

	switch {
	case strings.HasPrefix(spec, "DBX"):
		offset, _ = strconv.Atoi(spec[3:])
		bitOffset = 0
		if len(parts) >= 3 {
			bitOffset, _ = strconv.Atoi(parts[2])
		}
		size = 1
	case strings.HasPrefix(spec, "DBB"):
		offset, _ = strconv.Atoi(spec[3:])
		size = 1
	case strings.HasPrefix(spec, "DBW"):
		offset, _ = strconv.Atoi(spec[3:])
		size = 2
	case strings.HasPrefix(spec, "DBD"):
		offset, _ = strconv.Atoi(spec[3:])
		size = 4
	}
	return
}

func decodeS7Value(buf []byte, dataType string, bitOffset int) interface{} {
	if len(buf) == 0 {
		return nil
	}

	switch dataType {
	case "BOOL":
		if len(buf) > 0 {
			return (buf[0]>>uint(bitOffset))&1 == 1
		}
	case "INT":
		if len(buf) >= 2 {
			return float64(int16(buf[0])<<8 | int16(buf[1]))
		}
	case "UINT", "WORD":
		if len(buf) >= 2 {
			return float64(uint16(buf[0])<<8 | uint16(buf[1]))
		}
	case "DINT":
		if len(buf) >= 4 {
			return float64(int32(buf[0])<<24 | int32(buf[1])<<16 | int32(buf[2])<<8 | int32(buf[3]))
		}
	case "REAL":
		if len(buf) >= 4 {
			bits := uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3])
			return float64(uint32ToFloat32(bits))
		}
	}
	return nil
}

func uint32ToFloat32(bits uint32) float32 {
	return float32(int32(bits))
}

func (d *Driver) publishValues(values []tags.TagValue) {
	ctx := context.Background()
	for _, tv := range values {
		msg := &messaging.Message{
			Channel:   d.gateway.ChannelID,
			Domain:    d.gateway.DomainID,
			Subtopic:  tv.TagID,
			Publisher: d.gateway.ID,
			Protocol:  "s7",
			Payload:   tv.ToPayload(),
			Created:   tv.Timestamp,
		}
		if err := d.publisher.Publish(ctx, d.gateway.ChannelID, msg); err != nil {
			log.Printf("[s7] publish error: %v", err)
		}
	}
}

func (d *Driver) HandleWrite(cmd tags.WriteCommand) error {
	return fmt.Errorf("s7 write not yet implemented")
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
	log.Println("[s7-driver] starting...")

	gw := tags.GatewayConfig{
		ID:         getEnv("GATEWAY_ID", ""),
		Name:       getEnv("GATEWAY_NAME", "s7-gw"),
		DriverType: "s7",
		Host:       getEnv("PLC_HOST", "localhost"),
		Port:       getEnvInt("PLC_PORT", 102),
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

	rack := getEnvInt("S7_RACK", 0)
	slot := getEnvInt("S7_SLOT", 1)
	scanRate := getEnvInt("SCAN_RATE_MS", 1000)

	driver := NewDriver(gw, tagList, nil, rack, slot, scanRate)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := driver.Start(ctx); err != nil {
		log.Fatalf("s7 driver start: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[s7-driver] shutting down...")
	driver.Stop()
}
