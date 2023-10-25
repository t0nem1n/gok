package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const kafkaBrokerAddress = "localhost:29092"

var topic = kafka.TopicSpecification{
	Topic:             "test",
	NumPartitions:     10,
	ReplicationFactor: 1,
}

func ping(ctx context.Context, producer *kafka.Producer) error {
	adminClient, err := kafka.NewAdminClientFromProducer(producer)
	if err != nil {
		return err
	}
	defer adminClient.Close()
	clusterID, err := adminClient.ClusterID(ctx)
	if err != nil {
		return err
	}

	slog.Info("ClusterID:", slog.String("cluster_id", clusterID))
	return nil
}

func createProducer() (*kafka.Producer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family": "v4",
		"bootstrap.servers":     kafkaBrokerAddress,
		"security.protocol":     "PLAINTEXT",
		// "sasl.mechanisms":       "PLAIN",
		"partitioner": "consistent",
	})

	if err != nil {
		slog.Error(fmt.Sprintf("Failed to create producer: %s", err))
		return nil, err
	}

	// Delivery report handler for produced messages
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					slog.Info(
						"Delivery failed",
						slog.String("topic", *ev.TopicPartition.Topic),
						slog.Int("partition", int(ev.TopicPartition.Partition)),
						slog.String("error", ev.TopicPartition.Error.Error()),
					)
				} else {
					slog.Info(
						"Delivered message",
						slog.String("key", string(ev.Key)),
						slog.String("value", string(ev.Value)),
						slog.String("topic", *ev.TopicPartition.Topic),
						slog.Int("partition", int(ev.TopicPartition.Partition)),
					)
					slog.Debug(
						"Delivered message",
						slog.String("key", string(ev.Key)),
						slog.String("value", string(ev.Value)),
					)
				}
			}
		}
	}()

	err = ping(ctx, producer)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func push(producer *kafka.Producer) {
	for {
		if producer.IsClosed() {
			break
		}
		now := time.Now()
		err := producer.Produce(&kafka.Message{
			Key:            []byte(fmt.Sprintf("Key-A at %s", now.Format(time.TimeOnly))),
			Value:          []byte(fmt.Sprintf("Hello Go at %s!", now.Format(time.TimeOnly))),
			TopicPartition: kafka.TopicPartition{Topic: &topic.Topic, Partition: kafka.PartitionAny}}, nil)
		// Wait for message deliveries before shutting down
		if err != nil {
			slog.Error("Failed to produce message", slog.String("error", err.Error()))
		}
		time.Sleep(5 * time.Second)
	}
}

type consumerGroup struct {
	total    int
	consumer []*kafka.Consumer
}

func (c *consumerGroup) Close() {
	for _, consumer := range c.consumer {
		consumer.Close()
	}
}

func createConsumer() (*consumerGroup, error) {
	totalConsumer := 10
	consumerGroup := &consumerGroup{
		total: totalConsumer,
	}
	for i := 0; i < totalConsumer; i++ {
		id := fmt.Sprintf("myGroup-%d", i)
		slog.Info("Create consumer", slog.String("id", id))
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			// Avoid connecting to IPv6 brokers:
			// This is needed for the ErrAllBrokersDown show-case below
			// when using localhost brokers on OSX, since the OSX resolver
			// will return the IPv6 addresses first.
			// You typically don't need to specify this configuration property.
			"broker.address.family": "v4",
			"bootstrap.servers":     kafkaBrokerAddress,
			"security.protocol":     "PLAINTEXT",
			// "sasl.mechanisms":       "PLAIN",
			"group.id": "myGroup",
			//"group.instance.id": id,
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": "true",
		})
		if err != nil {
			return nil, err
		}
		err = consumer.Subscribe(topic.Topic, nil)
		if err != nil {
			// slog.Error("Failed to subscribe", slog.String("error", err.Error()))
			return nil, err
		}
		consumerGroup.consumer = append(consumerGroup.consumer, consumer)
	}

	return consumerGroup, nil
}

func consume(consumer *kafka.Consumer, id int) {
	for {
		if consumer.IsClosed() {
			break
		}

		msg, err := consumer.ReadMessage(-1)
		if err != nil && !err.(kafka.Error).IsTimeout() {
			slog.Error("Consumer error", slog.String("error", err.Error()))
		}
		if msg != nil {
			slog.Info("Receive message: ", slog.Int("id", id), slog.String("topic", *msg.TopicPartition.Topic), slog.Int("partition", int(msg.TopicPartition.Partition)), slog.String("key", string(msg.Key)), slog.String("value", string(msg.Value)))
		}
	}
}

func createAdmin() (*kafka.AdminClient, error) {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"broker.address.family": "v4",
		"bootstrap.servers":     kafkaBrokerAddress,
		"security.protocol":     "PLAINTEXT",
		// "sasl.mechanisms":       "PLAIN",
	})
	if err != nil {
		return nil, err
	}

	return admin, nil
}

func initTopic(admin *kafka.AdminClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{
		topic,
	})
	return err
}

func describeConsumerGroup(admin *kafka.AdminClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := admin.DescribeConsumerGroups(ctx, []string{"myGroup"}, nil)
	if err != nil {
		return err
	}
	for _, group := range res.ConsumerGroupDescriptions {
		slog.Info("Consumer group info", slog.Int("member count", len(group.Members)))
		for _, member := range group.Members {
			assignedPartitionInfo := []string{}
			for _, partition := range member.Assignment.TopicPartitions {
				assignedPartitionInfo = append(assignedPartitionInfo, partition.String())
			}
			slog.Info("Consumer group info",
				slog.String("group_id", group.GroupID),
				slog.String("member_id", member.ClientID),
				slog.String("member_id", member.GroupInstanceID),
				slog.Any("assigned partitions", assignedPartitionInfo),
			)
		}
	}
	return nil
}

func main() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)
	slog.Info("Testing with kafka")

	admin, err := createAdmin()
	if err != nil {
		panic(err)
	}
	err = initTopic(admin)
	if err != nil {
		panic(err)
	}

	producer, err := createProducer()
	if err != nil {
		panic(err)
	}

	consumerGroup, err := createConsumer()
	if err != nil {
		panic(err)
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			err = describeConsumerGroup(admin)
			if err != nil {
				panic(err)
			}
		}
	}()
	go push(producer)
	go func() {
		slog.Info("Start consumer", slog.Int("total", len(consumerGroup.consumer)))
		for i, consumer := range consumerGroup.consumer {
			go consume(consumer, i)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	sig := <-sigCh
	slog.Info("Received signal", slog.String("signal", sig.String()))
	slog.Info("Shutting down...")
	r := producer.Flush(15 * 1000)
	if r > 0 {
		slog.Info("Remaining %d message", r)
	}
	admin.Close()
	producer.Close()
	consumerGroup.Close()
	slog.Info("Shutdown complete")
}
