package tags

import (
	"encoding/json"
	"time"
)

type Tag struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Address     string `json:"address"`
	DataType    string `json:"data_type"`
	ScanRate    int    `json:"scan_rate_ms"`
	Unit        string `json:"unit,omitempty"`
	Description string `json:"description,omitempty"`
	Writable    bool   `json:"writable"`
	ChannelID   string `json:"channel_id"`
	Subtopic    string `json:"subtopic"`
}

type TagValue struct {
	TagID     string      `json:"tag_id"`
	Name      string      `json:"name"`
	Value     interface{} `json:"value"`
	Timestamp int64       `json:"ts"`
	Quality   string      `json:"quality"`
}

type GatewayConfig struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	DriverType string `json:"driver_type"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	DomainID   string `json:"domain_id"`
	ChannelID  string `json:"channel_id"`
}

type WriteCommand struct {
	TagID    string      `json:"tag_id"`
	Address  string      `json:"address"`
	Value    interface{} `json:"value"`
	DataType string      `json:"data_type"`
}

func (tv *TagValue) ToPayload() []byte {
	b, _ := json.Marshal(tv)
	return b
}

func NowNano() int64 {
	return time.Now().UnixNano()
}
