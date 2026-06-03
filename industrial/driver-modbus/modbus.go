package modbus

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
	modbuslib "github.com/simonvetter/modbus"
)

type Address struct {
	Type      string // "coil", "discrete", "input", "holding"
	Offset    uint16
	BitOffset *int
}

func ParseAddress(addr string) (Address, error) {
	addr = strings.TrimSpace(addr)
	if len(addr) < 5 {
		return Address{}, fmt.Errorf("invalid modbus address: %s", addr)
	}

	prefix := addr[:1]
	numStr := addr[1:]
	var bitOffset *int
	if idx := strings.Index(numStr, "."); idx >= 0 {
		bo, _ := strconv.Atoi(numStr[idx+1:])
		bitOffset = &bo
		numStr = numStr[:idx]
	}

	num, err := strconv.Atoi(numStr)
	if err != nil {
		return Address{}, fmt.Errorf("invalid modbus address: %s", addr)
	}

	var addrType string
	var offset uint16
	switch prefix {
	case "0":
		addrType = "coil"
		offset = uint16(num)
	case "1":
		addrType = "discrete"
		offset = uint16(num)
	case "3":
		addrType = "input"
		offset = uint16(num - 30001 + 1)
	case "4":
		addrType = "holding"
		offset = uint16(num - 40001 + 1)
	default:
		return Address{}, fmt.Errorf("unknown modbus prefix: %s", prefix)
	}

	return Address{Type: addrType, Offset: offset, BitOffset: bitOffset}, nil
}

type Driver struct {
	gateway    tags.GatewayConfig
	tags       []tags.Tag
	publisher  messaging.Publisher
	client     *modbuslib.ModbusClient
	stopChan   chan struct{}
	prevValues map[string]interface{}
	prevMu     sync.RWMutex
	cooldowns  map[string]time.Time
	cooldownMu sync.RWMutex
	scanRate   time.Duration
}

func NewDriver(gw tags.GatewayConfig, tagList []tags.Tag, pub messaging.Publisher, slaveID byte, scanRateMs int) *Driver {
	if scanRateMs <= 0 {
		scanRateMs = 1000
	}
	return &Driver{
		gateway:    gw,
		tags:       tagList,
		publisher:  pub,
		stopChan:   make(chan struct{}),
		prevValues: make(map[string]interface{}),
		cooldowns:  make(map[string]time.Time),
		scanRate:   time.Duration(scanRateMs) * time.Millisecond,
	}
}

func (d *Driver) Start(ctx context.Context) error {
	url := fmt.Sprintf("tcp://%s:%d", d.gateway.Host, d.gateway.Port)
	cfg := &modbuslib.ClientConfiguration{
		URL:     url,
		Timeout: 5 * time.Second,
	}

	client, err := modbuslib.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("modbus client init: %w", err)
	}

	if err := client.Open(); err != nil {
		return fmt.Errorf("modbus connect %s: %w", url, err)
	}

	d.client = client
	log.Printf("[modbus] connected to %s", url)

	go d.scanLoop(ctx)
	return nil
}

func (d *Driver) Stop() {
	close(d.stopChan)
	if d.client != nil {
		d.client.Close()
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
		if d.isOnCooldown(tag.ID) {
			continue
		}

		addr, err := ParseAddress(tag.Address)
		if err != nil {
			continue
		}

		val, quality := d.readTag(addr, tag.DataType)
		if quality == "GOOD" {
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
	}

	if len(values) > 0 {
		d.publishValues(values)
	}
}

func (d *Driver) readTag(addr Address, dataType string) (interface{}, string) {
	switch addr.Type {
	case "coil":
		val, err := d.client.ReadCoil(addr.Offset)
		if err != nil {
			return nil, "BAD"
		}
		return val, "GOOD"

	case "discrete":
		val, err := d.client.ReadDiscreteInput(addr.Offset)
		if err != nil {
			return nil, "BAD"
		}
		return val, "GOOD"

	case "input":
		return d.readRegister(addr, dataType, modbuslib.INPUT_REGISTER)

	case "holding":
		return d.readRegister(addr, dataType, modbuslib.HOLDING_REGISTER)
	}

	return nil, "BAD"
}

func (d *Driver) readRegister(addr Address, dataType string, regType modbuslib.RegType) (interface{}, string) {
	switch dataType {
	case "BOOL":
		reg, err := d.client.ReadRegister(addr.Offset, regType)
		if err != nil {
			return nil, "BAD"
		}
		if addr.BitOffset != nil {
			return (reg>>uint(*addr.BitOffset))&1 == 1, "GOOD"
		}
		return reg != 0, "GOOD"

	case "INT":
		reg, err := d.client.ReadRegister(addr.Offset, regType)
		if err != nil {
			return nil, "BAD"
		}
		return float64(int16(reg)), "GOOD"

	case "UINT":
		reg, err := d.client.ReadRegister(addr.Offset, regType)
		if err != nil {
			return nil, "BAD"
		}
		return float64(reg), "GOOD"

	case "DINT":
		val, err := d.client.ReadUint32(addr.Offset, regType)
		if err != nil {
			return nil, "BAD"
		}
		return float64(int32(val)), "GOOD"

	case "REAL":
		val, err := d.client.ReadFloat32(addr.Offset, regType)
		if err != nil {
			return nil, "BAD"
		}
		return float64(val), "GOOD"
	}

	return nil, "BAD"
}

func (d *Driver) publishValues(values []tags.TagValue) {
	ctx := context.Background()
	for _, tv := range values {
		msg := &messaging.Message{
			Channel:   d.gateway.ChannelID,
			Domain:    d.gateway.DomainID,
			Subtopic:  tv.TagID,
			Publisher: d.gateway.ID,
			Protocol:  "modbus",
			Payload:   tv.ToPayload(),
			Created:   tv.Timestamp,
		}
		if err := d.publisher.Publish(ctx, d.gateway.ChannelID, msg); err != nil {
			log.Printf("[modbus] publish error for tag %s: %v", tv.TagID, err)
		}
	}
}

func (d *Driver) HandleWrite(cmd tags.WriteCommand) error {
	addr, err := ParseAddress(cmd.Address)
	if err != nil {
		return err
	}

	switch cmd.DataType {
	case "BOOL":
		b := toBool(cmd.Value)
		if addr.Type == "coil" {
			return d.client.WriteCoil(addr.Offset, b)
		}
		if addr.Type == "holding" {
			reg, err := d.client.ReadRegister(addr.Offset, modbuslib.HOLDING_REGISTER)
			if err != nil {
				return err
			}
			mask := uint16(1 << uint(*addr.BitOffset))
			if b {
				return d.client.WriteRegister(addr.Offset, reg|mask)
			}
			return d.client.WriteRegister(addr.Offset, reg & ^mask)
		}

	case "INT", "UINT":
		if v, ok := toFloat(cmd.Value); ok {
			return d.client.WriteRegister(addr.Offset, uint16(int16(v)))
		}

	case "DINT":
		if v, ok := toFloat(cmd.Value); ok {
			return d.client.WriteUint32(addr.Offset, uint32(int32(v)))
		}

	case "REAL":
		if v, ok := toFloat(cmd.Value); ok {
			return d.client.WriteFloat32(addr.Offset, float32(v))
		}
	}

	return fmt.Errorf("unsupported write: %s to %s", cmd.DataType, addr.Type)
}

func (d *Driver) isOnCooldown(tagID string) bool {
	d.cooldownMu.RLock()
	defer d.cooldownMu.RUnlock()
	if ct, ok := d.cooldowns[tagID]; ok && time.Now().Before(ct) {
		return true
	}
	return false
}

func toBool(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return v
	case float64:
		return v != 0
	case int:
		return v != 0
	case string:
		lc := strings.ToLower(v)
		return lc == "true" || lc == "1" || lc == "on"
	default:
		return false
	}
}

func toFloat(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
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
	log.Println("[modbus-driver] starting...")

	gw := tags.GatewayConfig{
		ID:         getEnv("GATEWAY_ID", ""),
		Name:       getEnv("GATEWAY_NAME", "modbus-gw"),
		DriverType: "modbus",
		Host:       getEnv("PLC_HOST", "localhost"),
		Port:       getEnvInt("PLC_PORT", 502),
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

	slaveID := byte(getEnvInt("SLAVE_ID", 1))
	scanRate := getEnvInt("SCAN_RATE_MS", 1000)

	driver := NewDriver(gw, tagList, nil, slaveID, scanRate)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := driver.Start(ctx); err != nil {
		log.Fatalf("modbus driver start: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("[modbus-driver] shutting down...")
	driver.Stop()
}
