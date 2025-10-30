package service

import (
	"errors"
	"fmt"
	"log"
	"time"
	"trade-solution/ordercenter/model"
	"trade-solution/ordercenter/repository"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// InitDB 初始化数据库连接
func InitDB(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error), // 只打印错误，不打印SQL
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect DB: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	return db, nil
}

// MonitorAndReconnectDB 定时检查数据库连接并重连
func MonitorAndReconnectDB(dsn string, db **gorm.DB, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			sqlDB, err := (*db).DB()
			if sqlDB == nil || sqlDB.Ping() != nil || err != nil {
				log.Println("⚠️ 数据库连接失效，正在重连...")
				newDB, err := InitDB(dsn)
				if err != nil {
					log.Printf("❌ 重连失败: %v", err)
				} else {
					*db = newDB
					log.Println("✅ 数据库重连成功")
				}
			}
		}
	}()
}

type OrderService struct {
	repo *repository.OrderRepository
}

func NewOrderService(repo *repository.OrderRepository) *OrderService {
	return &OrderService{repo: repo}
}

func (s *OrderService) CreateOrder(order *model.OrderData) error {
	existing, err := s.repo.GetByID(order.OrderID)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("检查订单失败: %w", err)
	}
	if existing != nil && existing.OrderID != "" {
		return errors.New("订单已存在，order_id 重复")
	}
	return s.repo.Create(order)
}

func (s *OrderService) GetOrderByID(orderID string) (*model.OrderData, error) {
	return s.repo.GetByID(orderID)
}

func (s *OrderService) UpdateOrder(orderID string, updated *model.OrderData) error {
	return s.repo.Update(orderID, updated)
}

func (s *OrderService) DeleteOrder(orderID string) error {
	return s.repo.Delete(orderID)
}

func (s *OrderService) GetAllOrders() ([]model.OrderData, error) {
	return s.repo.GetAll()
}
