package utils

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

var rabbitMQInstance *RabbitMQ

type RabbitMQ struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Queues  map[string]amqp.Queue // 新增: 存储已声明队列
}

func GetRabbitMQInstance() (*RabbitMQ, error) {
	if rabbitMQInstance == nil {
		return nil, errors.New("RabbitMQ 实例未初始化，请先调用 InitRabbitMQ")
	}
	return rabbitMQInstance, nil
}

func InitRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("连接 RabbitMQ 失败: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("打开通道失败: %w", err)
	}
	return &RabbitMQ{
		Conn:    conn,
		Channel: ch,
		Queues:  make(map[string]amqp.Queue),
	}, nil
}

// 添加队列存在性检查方法
func (rmq *RabbitMQ) QueueExists(queue string) bool {
	_, exists := rmq.Queues[queue]
	return exists
}

// 声明交换机
func (rmq *RabbitMQ) DeclareExchange(exchange string) error {
	err := rmq.Channel.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("声明交换机失败: %w", err)
	}
	log.Printf("✅ 已声明交换机: %s\n", exchange)
	return nil
}

// 声明队列
func (rmq *RabbitMQ) DeclareQueue(queue string) (amqp.Queue, error) {
	q, err := rmq.Channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return q, fmt.Errorf("声明队列失败: %w", err)
	}
	log.Printf("✅ 已声明队列: %s\n", queue)
	return q, nil
}

// 声明队列
func (rmq *RabbitMQ) DeclareQueueQuorum(queue string) (amqp.Queue, error) {
	q, err := rmq.Channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-queue-type": "quorum",
		},
	)
	if err != nil {
		return q, fmt.Errorf("声明队列失败: %w", err)
	}
	log.Printf("✅ 已声明队列: %s\n", queue)
	return q, nil
}

// 队列绑定交换机
func (rmq *RabbitMQ) BindQueue(queue, exchange, routingKey string) error {
	err := rmq.Channel.QueueBind(
		queue,
		routingKey,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("绑定队列失败: %w", err)
	}
	log.Printf("✅ 队列 %s 已绑定到交换机 %s（routingKey: %s）\n", queue, exchange, routingKey)
	return nil
}

// 发布消息
func (rmq *RabbitMQ) Publish(exchange, routingKey string, body []byte, delayMs int) error {
	headers := amqp.Table{"x-delay": delayMs * 1000} // 延迟每秒
	err := rmq.Channel.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers:      headers,
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		return fmt.Errorf("发布消息失败: %w", err)
	}
	log.Printf("📤 已向交换机 %s（routingKey: %s）发布消息\n", exchange, routingKey)
	return nil
}

// 消费消息
func (rmq *RabbitMQ) Consume(queue string) (<-chan amqp.Delivery, error) {
	//// 每次只处理10条
	//if err := rmq.Channel.Qos(10, 0, false); err != nil {
	//	return nil, fmt.Errorf("设置Qos失败: %w", err)
	//}

	msgs, err := rmq.Channel.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("注册消费者失败: %w", err)
	}
	log.Printf("📥 开始消费队列: %s\n", queue)
	return msgs, nil
}

// 关闭连接
func (rmq *RabbitMQ) Close() {
	if rmq.Channel != nil {
		rmq.Channel.Close()
		log.Println("🔒 已关闭通道")
	}
	if rmq.Conn != nil {
		rmq.Conn.Close()
		log.Println("🔒 已关闭连接")
	}
}
func forceInitRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &RabbitMQ{Conn: conn, Channel: ch, Queues: make(map[string]amqp.Queue)}, nil
}
func MonitorAndReconnect(ctx context.Context, url string, onReconnect func(*RabbitMQ)) {
	for {
		rmq, err := InitRabbitMQ(url)
		if err != nil {
			log.Printf("❌ 初始化 RabbitMQ 失败: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		if onReconnect != nil {
			onReconnect(rmq)
		}

		errChan := make(chan *amqp.Error, 1)
		rmq.Conn.NotifyClose(errChan)

		select {
		case <-ctx.Done():
			rmq.Close()
			log.Println("RabbitMQ 监控退出")
			return
		case err := <-errChan:
			log.Printf("⚠️ RabbitMQ 连接关闭: %v，准备重连", err)
			rmq.Close()
			time.Sleep(3 * time.Second)
		}
	}
}
