package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"trade-solution/common/go/lib/models"
	"trade-solution/ordercenter/model"
	"trade-solution/ordercenter/repository"
	"trade-solution/ordercenter/utils"

	"gorm.io/gorm"
)

// 并发 worker
func StartMultiStrategyConsumer(queueName string, rmq *utils.RabbitMQ, workerCount int, prefetch int, db *gorm.DB) error {
	// 设置 QoS：每个 worker 最多同时处理 prefetch 条未确认的消息
	if err := rmq.Channel.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("设置 Qos 失败: %w", err)
	}

	deliveries, err := rmq.Consume(queueName)
	if err != nil {
		return fmt.Errorf("从队列 %s 消费失败: %w", queueName, err)
	}
	log.Printf("📥 开始消费队列: %s，workers=%d prefetch=%d", queueName, workerCount, prefetch)

	ctx := context.Background()

	// 启动多个 worker 并发处理消息
	for i := 0; i < workerCount; i++ {
		workerID := i
		repo := repository.NewOrderRepository(db.Session(&gorm.Session{NewDB: true}))
		srv := NewOrderService(repo)

		go func(workerID int, srv *OrderService) {
			for d := range deliveries {
				// 处理单条消息
				if err := handleMessage(ctx, d.Body, rmq, srv); err != nil {
					log.Printf("❌ worker-%d 处理消息失败: %v", workerID, err)
					//// —— 新增 debug 打印 ——
					//log.Printf("🛠 原始 JSON：%s", d.Body)
					d.Nack(false, false) // 拒绝并重新入队
					continue
				}
				d.Ack(false) // 成功确认
			}
		}(workerID, srv)
	}

	return nil
}

// 单条消息处理逻辑
func handleMessage(ctx context.Context, body []byte, rmq *utils.RabbitMQ, srv *OrderService) error {
	var msg models.Order
	if err := json.Unmarshal(body, &msg); err != nil {
		return fmt.Errorf("无法解析消息: %w", err)
	}

	order := &model.OrderData{
		OrderID:        msg.OrderInfo.OrderID,
		Status:         msg.OrderInfo.Status,
		EventTimestamp: msg.OrderInfo.EventTimestamp,
		EventType:      msg.OrderInfo.EventType,

		StrategyID:   int64(msg.StrategyInfo.StrategyId),
		UserID:       msg.StrategyInfo.UserInfo.UserId,
		BscPublicKey: msg.StrategyInfo.UserInfo.BscPublicKey,
		SolPublicKey: msg.StrategyInfo.UserInfo.SolPublicKey,

		TokenAddress: msg.ChainInfo.TokenAddress,
		ChainIndex:   msg.ChainInfo.ChainIndex,

		Metadata: model.JSONB{
			"order": msg,
		},

		//Metadata: model.JSONB{
		//	"order": msg,
		//},
		//CreatedAt: time.Now(),
		//UpdatedAt: time.Now(),
	}

	// 写入数据库（捕获重复主键错误）
	if err := srv.CreateOrder(order); err != nil {
		if strings.Contains(err.Error(), "订单已存在") {
			log.Printf("⚠️ 订单已存在，跳过：%s", order.OrderID)
			return nil
		}
		return fmt.Errorf("写入数据库失败: %w", err)
	}

	log.Printf("✅ 成功写入订单：%s", order.OrderID)

	/*查数据库 在发布？*/
	/*		var fullOrder model.OrderData
			if err := repo.DB.WithContext(ctx).First(&fullOrder, "order_id = ?", order.OrderID).Error; err != nil {
				return fmt.Errorf("查询数据库失败: %w", err)
			}
		bodyData, err := json.Marshal(fullOrder.Metadata["order"])*/

	// 推入下游队列 交换机
	pushExchange := "order_push_exchange"
	routingKey := "new_order"

	bodyData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化推送内容失败: %w", err)
	}

	if err := rmq.Publish(pushExchange, routingKey, bodyData, 0); err != nil {
		return fmt.Errorf("发布到交换机失败: %w", err)
	}

	log.Printf("📤 已推送至交换机 %s (routingKey=%s): orderId=%s userId=%s", pushExchange, routingKey, order.OrderID, msg.StrategyInfo.UserInfo.UserId)

	return nil
}
