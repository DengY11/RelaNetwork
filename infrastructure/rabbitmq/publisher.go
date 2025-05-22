package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// Publisher 结构体用于发布消息
type Publisher struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchangeName string
	logger       *zap.Logger
}

// NewPublisher 创建一个新的 Publisher 实例
// amqpURL: RabbitMQ 连接字符串, e.g., "amqp://user:pass@host:port/vhost"
// exchangeName: 要声明并使用的交换机名称
// logger: zap logger 实例
func NewPublisher(amqpURL string, exchangeName string, logger *zap.Logger) (*Publisher, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		logger.Error("无法连接到 RabbitMQ", zap.String("url", amqpURL), zap.Error(err))
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close() // 如果打开通道失败，关闭连接
		logger.Error("无法打开 RabbitMQ 通道", zap.Error(err))
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	// 声明一个交换机 (例如，direct 类型)
	// 你可以根据需要将其改为 topic 或 fanout
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		true,         // durable: 交换机在 RabbitMQ 重启后依然存在
		false,        // auto-deleted: 当没有队列绑定到这个交换机时，不会自动删除
		false,        // internal: false 表示可以被客户端直接发布消息
		false,        // no-wait: false 表示等待服务器确认交换机已成功声明
		nil,          // arguments
	)
	if err != nil {
		ch.Close() // 如果声明交换机失败，关闭通道和连接
		conn.Close()
		logger.Error("无法声明 RabbitMQ 交换机", zap.String("exchange", exchangeName), zap.Error(err))
		return nil, fmt.Errorf("failed to declare an exchange '%s': %w", exchangeName, err)
	}
	logger.Info("RabbitMQ 交换机声明成功", zap.String("exchange", exchangeName), zap.String("type", "direct"))

	return &Publisher{
		conn:         conn,
		channel:      ch,
		exchangeName: exchangeName,
		logger:       logger.Named("rabbitmq_publisher"), // 给 logger 一个名字以区分
	}, nil
}

// Publish 发布消息到指定的 routingKey
// routingKey: 消息的路由键
// messageBody: 任何可以被 json.Marshal 的结构体或数据
func (p *Publisher) Publish(ctx context.Context, routingKey string, messageBody interface{}) error {
	body, err := json.Marshal(messageBody)
	if err != nil {
		p.logger.Error("消息序列化为 JSON 失败", zap.Any("message", messageBody), zap.Error(err))
		return fmt.Errorf("failed to marshal message to JSON: %w", err)
	}

	err = p.channel.PublishWithContext(ctx,
		p.exchangeName, // exchange
		routingKey,     // routing key
		false,          // mandatory: 如果为true，当消息不能路由到任何队列时，会返回给生产者
		false,          // immediate: 如果为true，当没有消费者连接到匹配的队列时，消息会返回给生产者 (RabbitMQ 3.0+ 已废弃此参数)
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent, // 使消息持久化 (写入磁盘)
		},
	)
	if err != nil {
		p.logger.Error("发布消息到 RabbitMQ 失败",
			zap.String("exchange", p.exchangeName),
			zap.String("routingKey", routingKey),
			zap.Error(err),
		)
		return fmt.Errorf("failed to publish message: %w", err)
	}

	p.logger.Info("消息成功发布到 RabbitMQ",
		zap.String("exchange", p.exchangeName),
		zap.String("routingKey", routingKey),
		// zap.ByteString("body", body), // 出于安全和日志大小考虑，可能不记录完整 body
	)
	return nil
}

// Close 关闭 Publisher 的通道和连接
func (p *Publisher) Close() {
	if p.channel != nil {
		err := p.channel.Close()
		if err != nil {
			p.logger.Error("关闭 RabbitMQ 通道失败", zap.Error(err))
		}
	}
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			p.logger.Error("关闭 RabbitMQ 连接失败", zap.Error(err))
		}
	}
	p.logger.Info("RabbitMQ Publisher 已关闭")
}
