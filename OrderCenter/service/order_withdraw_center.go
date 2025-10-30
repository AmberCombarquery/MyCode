package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"trade-solution/common/go/lib/models"
	"trade-solution/ordercenter/model"
	"trade-solution/ordercenter/repository"
	"trade-solution/ordercenter/utils"

	"gorm.io/gorm"
)

// 并发 worker
func StartWithdrawConsumer(queueName string, rmq *utils.RabbitMQ, workerCount int, prefetch int, db *gorm.DB) error {
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
				if err := withdrawMessage(ctx, d.Body, srv); err != nil {
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
func withdrawMessage(ctx context.Context, body []byte, srv *OrderService) error {
	var msg models.Order
	if err := json.Unmarshal(body, &msg); err != nil {
		return fmt.Errorf("无法解析消息: %w", err)
	}

	orderID := msg.OrderInfo.OrderID
	if orderID == "" {
		return fmt.Errorf("无效的订单ID")
	}

	// 更新数据库（删除，更新）
	// 查询订单是否存在
	existing, err := srv.GetOrderByID(orderID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("⚠️ 撤单忽略：订单 %s 不存在", orderID)
			return nil // 不算错误
		}
		return fmt.Errorf("查询订单失败: %w", err)
	}

	if msg.OrderInfo.Status == "update" {
		updateData := &model.OrderData{
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
		if err := srv.UpdateOrder(orderID, updateData); err != nil {
			return fmt.Errorf("更新订单失败: %w", err)
		}
		log.Printf("♻️ 成功更新订单 %s 状态为 %s", orderID, msg.OrderInfo.Status)
		return nil // 不算错误
	}

	// 删除
	if err := srv.DeleteOrder(orderID); err != nil {
		return fmt.Errorf("删除订单失败: %w", err)
	}
	log.Printf("🗑️ 成功删除订单：%s (原状态: %s)", orderID, existing.Status)
	return nil
}
