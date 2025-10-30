package repository

import (
	"trade-solution/ordercenter/model"

	"gorm.io/gorm"
)

type OrderRepository struct {
	DB *gorm.DB
}

func NewOrderRepository(db *gorm.DB) *OrderRepository {
	return &OrderRepository{DB: db}
}

func (r *OrderRepository) Create(order *model.OrderData) error {
	return r.DB.Create(order).Error
}

func (r *OrderRepository) GetByID(orderID string) (*model.OrderData, error) {
	var order model.OrderData
	err := r.DB.First(&order, "order_id = ?", orderID).Error
	return &order, err
}

func (r *OrderRepository) Update(orderID string, updated *model.OrderData) error {
	return r.DB.Model(&model.OrderData{}).Where("order_id = ?", orderID).Updates(updated).Error
}

func (r *OrderRepository) Delete(orderID string) error {
	return r.DB.Delete(&model.OrderData{}, "order_id = ?", orderID).Error
}

func (r *OrderRepository) GetAll() ([]model.OrderData, error) {
	var orders []model.OrderData
	err := r.DB.Find(&orders).Error
	return orders, err
}
