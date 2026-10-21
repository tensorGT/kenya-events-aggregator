package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	TopicCrawlJobs        = "crawl.jobs"
	TopicEventsDiscovered = "events.discovered"
	TopicEventsDLQ        = "events.dlq"
)

type Client struct {
	producer *kgo.Client
	brokers  []string
}

func New(brokers []string) (*Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client init: %w", err)
	}
	return &Client{producer: cl, brokers: brokers}, nil
}

func (c *Client) EnsureTopics(ctx context.Context) error {
	adm := kadm.NewClient(c.producer)
	_, err := adm.CreateTopics(ctx, 3, 1, nil, TopicCrawlJobs, TopicEventsDiscovered, TopicEventsDLQ)
	return err
}

func (c *Client) Publish(ctx context.Context, topic, key string, value []byte) error {
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.producer.ProduceSync(ctx, rec).FirstErr()
}

func (c *Client) NewConsumer(groupID, topic string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(c.brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
}

func (c *Client) Close() {
	c.producer.Close()
}
