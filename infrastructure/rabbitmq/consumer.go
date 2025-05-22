package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// MessageHandler 是一个函数类型，用于处理接收到的 RabbitMQ 消息
// 返回的 error 如果非 nil，将决定消息是否被 Nack (并可能重入队列或死信)
type MessageHandler func(ctx context.Context, delivery amqp.Delivery) error

// Consumer 结构体用于消费消息
type Consumer struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	queueName    string
	consumerTag  string // 唯一的消费者标签
	logger       *zap.Logger
	done         chan error // 用于通知消费者已停止
	handler      MessageHandler
	autoAck      bool   // 是否自动确认消息
	exchangeName string // 消费者需要知道从哪个交换机绑定队列
	routingKey   string // 消费者需要知道用什么路由键绑定队列
}

// ConsumerOptions 用于配置 Consumer
type ConsumerOptions struct {
	ExchangeName string // 必须: 绑定的交换机名称
	ExchangeType string // 可选: 交换机类型 (direct, topic, fanout, headers), 默认为 "direct"
	QueueName    string // 必须: 队列名称 (如果为空，将生成一个临时队列名)
	RoutingKey   string // 必须: 绑定队列到交换机的路由键
	ConsumerTag  string // 可选: 消费者标签 (如果为空，将生成一个)
	AutoAck      bool   // 可选: 是否自动确认消息，默认为 false (手动确认)
	DurableQueue bool   // 可选: 队列是否持久化，默认为 true
	Exclusive    bool   // 可选: 是否为排他队列，默认为 false
	NoWait       bool   // 可选: 声明队列和绑定时不等待服务器确认，默认为 false
}

// NewConsumer 创建一个新的 Consumer 实例并开始消费消息
// amqpURL: RabbitMQ 连接字符串
// handler: 消息处理函数
// opts: Consumer 配置选项
// logger: zap logger 实例
func NewConsumer(amqpURL string, handler MessageHandler, opts ConsumerOptions, logger *zap.Logger) (*Consumer, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		logger.Error("无法连接到 RabbitMQ", zap.String("url", amqpURL), zap.Error(err))
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		logger.Error("无法打开 RabbitMQ 通道", zap.Error(err))
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	if opts.ExchangeType == "" {
		opts.ExchangeType = "direct" // 默认交换机类型
	}
	// 声明交换机 (确保它存在)
	err = ch.ExchangeDeclare(
		opts.ExchangeName,
		opts.ExchangeType,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		logger.Error("无法声明 RabbitMQ 交换机", zap.String("exchange", opts.ExchangeName), zap.Error(err))
		return nil, fmt.Errorf("failed to declare exchange '%s': %w", opts.ExchangeName, err)
	}

	// 声明队列
	q, err := ch.QueueDeclare(
		opts.QueueName,    // name: 如果为空，服务器将生成一个唯一的名称
		opts.DurableQueue, // durable: true 意味着队列在服务器重启后仍然存在
		false,             // delete when unused: false 表示当没有消费者时队列不会被删除 (除非 opts.QueueName 为空)
		opts.Exclusive,    // exclusive: true 意味着队列只能被此连接访问，并在连接关闭时删除
		opts.NoWait,       // no-wait: false 表示等待服务器确认队列已成功声明
		nil,               // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		logger.Error("无法声明 RabbitMQ 队列", zap.String("queue", opts.QueueName), zap.Error(err))
		return nil, fmt.Errorf("failed to declare queue '%s': %w", opts.QueueName, err)
	}
	actualQueueName := q.Name // 获取实际的队列名 (如果 opts.QueueName 为空，则由服务器生成)
	logger.Info("RabbitMQ 队列声明成功", zap.String("queue", actualQueueName), zap.Bool("durable", opts.DurableQueue))

	// 将队列绑定到交换机
	err = ch.QueueBind(
		actualQueueName,   // queue name
		opts.RoutingKey,   // routing key
		opts.ExchangeName, // exchange
		opts.NoWait,       // no-wait
		nil,               // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		logger.Error("无法将队列绑定到交换机",
			zap.String("queue", actualQueueName),
			zap.String("exchange", opts.ExchangeName),
			zap.String("routingKey", opts.RoutingKey),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to bind queue '%s' to exchange '%s' with key '%s': %w", actualQueueName, opts.ExchangeName, opts.RoutingKey, err)
	}
	logger.Info("RabbitMQ 队列成功绑定到交换机",
		zap.String("queue", actualQueueName),
		zap.String("exchange", opts.ExchangeName),
		zap.String("routingKey", opts.RoutingKey),
	)

	consumerTag := opts.ConsumerTag
	if consumerTag == "" {
		consumerTag = fmt.Sprintf("consumer-%s-%d", actualQueueName, time.Now().UnixNano())
	}

	consumer := &Consumer{
		conn:         conn,
		channel:      ch,
		queueName:    actualQueueName,
		consumerTag:  consumerTag,
		handler:      handler,
		autoAck:      opts.AutoAck,
		exchangeName: opts.ExchangeName,
		routingKey:   opts.RoutingKey,
		logger:       logger.Named("rabbitmq_consumer").With(zap.String("queue", actualQueueName), zap.String("tag", consumerTag)),
		done:         make(chan error),
	}

	go consumer.startConsuming() // 在 goroutine 中开始消费
	logger.Info("RabbitMQ Consumer 已启动并开始监听消息")

	return consumer, nil
}

func (c *Consumer) startConsuming() {
	deliveries, err := c.channel.Consume(
		c.queueName,   // queue
		c.consumerTag, // consumer tag
		c.autoAck,     // auto-ack
		false,         // exclusive
		false,         // no-local (not supported by RabbitMQ)
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		c.logger.Error("启动消费者失败", zap.Error(err))
		c.done <- fmt.Errorf("failed to start consuming: %w", err)
		return
	}

	c.logger.Info("等待 RabbitMQ 消息...")

	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				c.logger.Info("消息通道已关闭，消费者正在停止")
				c.done <- nil // 正常关闭
				return
			}
			ctx := context.Background() // 或者从其他地方传递 context
			c.logger.Debug("收到消息", zap.ByteString("body", delivery.Body))
			err := c.handler(ctx, delivery)
			if !c.autoAck {
				if err != nil {
					c.logger.Error("消息处理失败，将发送 Nack", zap.Error(err), zap.Bool("requeue", false))
					// 考虑是否需要 requeue。如果错误是可重试的，可以设为 true。
					// 频繁 requeue 无法处理的消息可能导致死循环。
					if ackErr := delivery.Nack(false, false); ackErr != nil { // multiple, requeue
						c.logger.Error("发送 Nack 失败", zap.Error(ackErr))
					}
				} else {
					c.logger.Debug("消息处理成功，将发送 Ack")
					if ackErr := delivery.Ack(false); ackErr != nil { // multiple
						c.logger.Error("发送 Ack 失败", zap.Error(ackErr))
					}
				}
			}
		case <-c.done: // 用于外部信号关闭
			c.logger.Info("收到关闭信号，消费者正在停止")
			return
		}
	}
}

// Shutdown 优雅地关闭消费者
// 它会取消消费者注册，并等待当前正在处理的消息完成（如果适用）
func (c *Consumer) Shutdown() error {
	if c.channel == nil {
		return fmt.Errorf("consumer channel is nil, cannot shutdown")
	}

	// 取消消费者标签，这将停止向此消费者发送新消息
	c.logger.Info("正在取消 RabbitMQ 消费者标签", zap.String("tag", c.consumerTag))
	err := c.channel.Cancel(c.consumerTag, false) // noWait=false，等待服务器确认取消
	if err != nil {
		c.logger.Error("取消 RabbitMQ 消费者失败", zap.String("tag", c.consumerTag), zap.Error(err))
		// 即使取消失败，我们仍然尝试关闭通道和连接
	}

	// 发送信号给消费循环以使其停止
	// 如果消费循环因为通道关闭等原因已经停止，这可能是无操作的
	select {
	case c.done <- fmt.Errorf("shutdown initiated"): // 发送信号，如果还没关闭的话
	default: // 如果 c.done 已经关闭或满了，则不阻塞
	}

	// 等待消费循环完成
	// 可以设置一个超时以防止无限期阻塞
	select {
	case errFromDone := <-c.done:
		if errFromDone != nil && errFromDone.Error() != "shutdown initiated" {
			c.logger.Warn("消费者循环退出时发生错误", zap.Error(errFromDone))
		}
	case <-time.After(10 * time.Second): // 超时保护
		c.logger.Warn("等待消费者循环完成超时")
	}

	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			c.logger.Error("关闭 RabbitMQ 通道失败", zap.Error(err))
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			c.logger.Error("关闭 RabbitMQ 连接失败", zap.Error(err))
		}
	}

	c.logger.Info("RabbitMQ Consumer 已成功关闭")
	return err // 返回 cancel 的错误，或者 nil
}
