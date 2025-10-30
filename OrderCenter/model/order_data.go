package model

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

type JSONB map[string]interface{}

func (j JSONB) Value() (driver.Value, error) {
	return json.Marshal(j)
}

func (j *JSONB) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, j)
}

func (j *JSONB) MarshalJSON() ([]byte, error) {
	if j == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(map[string]interface{}(*j))
}

type OrderData struct {
	OrderID        string `gorm:"column:order_id;primaryKey" json:"order_id"`
	Status         string `gorm:"column:status" json:"status"`
	EventTimestamp int64  `gorm:"column:event_timestamp" json:"event_timestamp"`

	StrategyID   int64  `gorm:"column:strategy_id" json:"strategy_id"`
	UserID       string `gorm:"column:user_id" json:"user_id"`
	BscPublicKey string `gorm:"column:bsc_public_key" json:"bsc_public_key"`
	SolPublicKey string `gorm:"column:sol_public_key" json:"sol_public_key"`

	TokenAddress string `gorm:"column:token_address" json:"token_address"`
	ChainIndex   int    `gorm:"column:chain_index" json:"chain_index"`
	EventType    string `gorm:"column:event_type" json:"event_type"`

	Metadata  JSONB     `gorm:"column:metadata;type:json" json:"metadata"`
	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (OrderData) TableName() string {
	return "order_data"
}
