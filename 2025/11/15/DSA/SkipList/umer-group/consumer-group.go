package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	// 配置消费者组
	config := sarama.NewConfig()
	config.Version = sarama.V4_1_0_0 // 与集群版本匹配

	// 偏移量配置：默认自动提交（可改为手动提交）
	config.Consumer.Offsets.AutoCommit.Enable = true      // 开启自动提交
	config.Consumer.Offsets.AutoCommit.Interval = 5000    // 自动提交间隔（毫秒）
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // 初始偏移量（首次消费时）
	config.Consumer.Return.Errors = true

	// 创建消费组
	groupID := "auto-offset-group" // 消费组唯一标识
	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{"localhost:9092"}, // Kafka 地址
		groupID,
		config,
	)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	// 要消费的主题
	topics := []string{"test"}
	consumer := &GroupConsumer{}

	go func() {
		for {
			// 持续消费（重平衡后会重新调用）
			if err := consumerGroup.Consume(context.Background(), topics, consumer); err != nil {
				log.Printf("Consume error: %v", err)
				return
			}
		}
	}()

	// 等待中断信号
	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, syscall.SIGINT, syscall.SIGTERM)
	<-signChan

	fmt.Println("Shutting down consumer group...")
}

// GroupConsumer 自定义消费者，实现 sarama.ConsumerGroupHandler 接口
type GroupConsumer struct{}

// Setup 在分区分配完成后调用（初始化）
func (c *GroupConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 在分区被重新分配前调用（清理）
func (c *GroupConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 处理分区消息（核心逻辑）
func (c *GroupConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Received message: %s (topic: %s, partition: %d, offset: %d)\n",
			string(msg.Value), msg.Topic, msg.Partition, msg.Offset)

		// 手动提交偏移量（可选，若关闭自动提交则必须手动调用）
		// 注意：需在消息处理成功后再提交，避免消息丢失
		session.MarkMessage(msg, "") // 标记消息为已处理，会在 session 结束时提交
	}
	return nil
}
