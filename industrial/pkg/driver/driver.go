package driver

import (
	"context"

	"github.com/absmach/magistrala/industrial/pkg/tags"
	"github.com/absmach/magistrala/pkg/messaging"
)

type Driver interface {
	Start(ctx context.Context) error
	Stop() error
	PublishValues(values []tags.TagValue)
	HandleWrite(cmd tags.WriteCommand) error
}

type BaseDriver struct {
	Gateway   tags.GatewayConfig
	Tags      []tags.Tag
	Publisher messaging.Publisher
}

func (d *BaseDriver) PublishValues(values []tags.TagValue) {
	ctx := context.Background()
	for _, tv := range values {
		msg := &messaging.Message{
			Channel:   d.Gateway.ChannelID,
			Domain:    d.Gateway.DomainID,
			Subtopic:  tv.TagID,
			Publisher: d.Gateway.ID,
			Protocol:  "modbus",
			Payload:   tv.ToPayload(),
			Created:   tags.NowNano(),
		}
		d.Publisher.Publish(ctx, d.Gateway.ChannelID, msg)
	}
}

func (d *BaseDriver) Stop() {}
