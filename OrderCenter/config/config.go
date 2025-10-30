package config

import (
	"log"
	"os"
)

type Config struct {
	MySQLDSN    string
	RabbitMQURL string
}

var cfg *Config

func LoadConfig() *Config {
	if cfg != nil {
		return cfg
	}

	mysqlDSN := os.Getenv("MYSQL_DSN")
	rabbitmqURL := os.Getenv("RABBITMQ_URL")

	if mysqlDSN == "" || rabbitmqURL == "" {
		log.Fatal("MYSQL_DSN 或 RABBITMQ_URL 环境变量未设置")
	}

	cfg = &Config{
		MySQLDSN:    mysqlDSN,
		RabbitMQURL: rabbitmqURL,
	}
	return cfg
}
