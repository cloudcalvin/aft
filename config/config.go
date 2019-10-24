package config

import (
	"io/ioutil"
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

type AftConfig struct {
	ConsistencyType string   `yaml:"consistencyType"`
	StorageType     string   `yaml:"storageType"`
	IpAddress       string   `yaml:"ipAddress"`
	ReplicaList     []string `yaml:"replicaList"`
	ManagerAddress  string   `yaml:"managerAddress"`
}

func ParseConfig(filename string) *AftConfig {
	bts, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal("Unable to read aft-config.yml. Please make sure that the config is properly configured and retry:\n%v", err)
		os.Exit(1)
	}

	var config AftConfig
	err = yaml.Unmarshal(bts, &config)
	if err != nil {
		log.Fatal("Unable to correctly parse aft-config.yml. Please check the config file and retry:\n%v", err)
		os.Exit(1)
	}

	return &config
}
