package configs

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

func init() {
	godotenv.Load()
}

func GetEnv(key string) (string, error) {
	value := os.Getenv("DB_CONNECTION")
	var err error
	if value == "" {
		err = fmt.Errorf("failed to find env variable: %s", key)
	}
	return value, err
}
