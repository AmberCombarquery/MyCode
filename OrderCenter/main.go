package main

import (
	"context"
	"log"
	"time"
	"trade-solution/ordercenter/config"
	"trade-solution/ordercenter/handler"
	"trade-solution/ordercenter/repository"
	"trade-solution/ordercenter/service"
	"trade-solution/ordercenter/utils"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	// 加载.env文件
	if err := godotenv.Load("ordercenter/config/.env"); err != nil {
		//if err := godotenv.Load("config/.env"); err != nil {
		log.Fatal("加载.env文件失败")
	}
	cfg := config.LoadConfig()

	// 初始化数据库
	db, err := service.InitDB(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("初始化数据库失败: %v", err)
	}

	// 启动监控和自动重连
	go service.MonitorAndReconnectDB(cfg.MySQLDSN, &db, 30*time.Second)

	// 初始化RabbitMQ
	rabbitMQ, err := utils.InitRabbitMQ(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("初始化RabbitMQ失败: %v", err)
	}

	// 声明交换机
	err = rabbitMQ.DeclareExchange(
		"basic_info_exchange", // 交换机名称
	)
	if err != nil {
		log.Fatalf("声明交换机失败: %v", err)
	}
	_, err = rabbitMQ.DeclareQueueQuorum("multi_strategy_on_one_token_queue")
	if err != nil {
		log.Fatalf("声明队列失败: %v", err)
	}

	_, err = rabbitMQ.DeclareQueueQuorum("withdraw_order_queue")
	if err != nil {
		log.Fatalf("声明队列失败: %v", err)
	}

	// 绑定队列
	routingKey := "basic_info"
	err = rabbitMQ.BindQueue("multi_strategy_on_one_token_queue", "basic_info_exchange", routingKey)
	if err != nil {
		log.Fatalf("绑定队列失败: %v", err)
	}

	if err := rabbitMQ.DeclareExchange("order_push_exchange"); err != nil {
		log.Fatalf("声明交换机失败: %v", err)
	}
	// （可选）绑定队列
	if _, err := rabbitMQ.DeclareQueueQuorum("order_push_queue"); err != nil {
		log.Fatalf("声明队列失败: %v", err)
	}
	if err := rabbitMQ.BindQueue("order_push_queue", "order_push_exchange", "new_order"); err != nil {
		log.Fatalf("绑定队列失败: %v", err)
	}

	// 依赖注入
	orderRepo := repository.NewOrderRepository(db)
	orderSrv := service.NewOrderService(orderRepo)

	// 启动 RabbitMQ 断线重连监听
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go utils.MonitorAndReconnect(ctx, cfg.RabbitMQURL, func(rmq *utils.RabbitMQ) {
		//// 重连时需要重新声明队列和绑定
		rabbitMQ = rmq
		// 重新声明队列（可选）
		rmq.DeclareExchange("basic_info_exchange")
		rmq.DeclareQueueQuorum("multi_strategy_on_one_token_queue")
		rmq.DeclareQueueQuorum("order_queue")
		rmq.DeclareQueueQuorum("order_active_queue")

		rmq.DeclareExchange("order_push_exchange")
		rmq.DeclareQueueQuorum("order_push_queue")
		rmq.BindQueue("order_push_queue", "order_push_exchange", "new_order")

		rmq.DeclareQueueQuorum("withdraw_order_queue")

		rmq.BindQueue("multi_strategy_on_one_token_queue", "basic_info_exchange", "basic_info")
		// 启动消费者
		go service.StartMultiStrategyConsumer("multi_strategy_on_one_token_queue", rabbitMQ, 10, 10, db)
		//go service.StartMultiStrategyConsumer("invalid_order_queue", rabbitMQ, 10, 10, db)

		go service.StartWithdrawConsumer("withdraw_order_queue", rabbitMQ, 10, 10, db)
	})

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), cors.Default(), gzip.Gzip(gzip.DefaultCompression))

	// 注册路由
	handler.RegisterOrderRoutes(r, orderSrv)

	log.Println("🚀 服务器启动在端口 :8016")
	r.Run(":8016")
}
