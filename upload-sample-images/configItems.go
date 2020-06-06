package main

import "github.com/spf13/viper"

type sourceParams struct {
	FolderPath string
}
type localDriveParams struct {
	FolderPath string
}
type storageParams struct {
	ContainerName string
}
type destinationParams struct {
	BlobStorage storageParams
	LocalDrive  localDriveParams
}
type labelParams struct {
	Fontfile  string
	Dpi       float64
	Size      float64
	PositionX int
	PositionY int
}

type serviceConfig struct {
	Source           sourceParams
	Destination      destinationParams
	Label            labelParams
	NeededImageCount int
}

func loadConfig() (*serviceConfig, error) {
	viper.SetConfigFile("config.yaml")
	var config = serviceConfig{}
	var err = viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	viper.Unmarshal(&config)
	return &config, nil
}
