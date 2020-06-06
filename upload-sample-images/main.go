package main

import (
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"io/ioutil"
	"log"
	"os"

	"github.com/golang/freetype"
	"github.com/spf13/viper"
)

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

func main() {
	config, err := loadConfig()
	if err != nil {
		panic(err)
	}

	var fontBytes, _ = ioutil.ReadFile(config.Label.Fontfile)
	font, err := freetype.ParseFont(fontBytes)
	if err != nil {
		log.Println(err)
		return
	}
	fontForeground := image.Black

	var storageAccountKey = azureBlobKeys{
		AccessKey:     os.Getenv("AZURE_ACCESS_KEY"),
		AccountName:   os.Getenv("AZURE_STORAGE_ACCOUNT"),
		ContainerName: config.Destination.BlobStorage.ContainerName,
	}

	files, _ := ioutil.ReadDir(config.Source.FolderPath)
	var fileCounter = 0
	for i := 0; i < config.NeededImageCount; i++ {
		var fileInfo = files[fileCounter]
		if fileCounter < len(files)-1 {
			fileCounter++
		} else {
			fileCounter = 0
		}

		var file, _ = os.Open(config.Source.FolderPath + "\\" + fileInfo.Name())
		defer file.Close()
		var img, _, _ = image.Decode(file)
		var numberString = fmt.Sprintf("%03d", i)
		var targetFileName = config.Destination.LocalDrive.FolderPath + "\\" + numberString + ".jpeg"

		var bounds = img.Bounds()
		rgba := image.NewRGBA(image.Rect(0, 0, bounds.Dx(), bounds.Dy()))
		draw.Draw(rgba, rgba.Bounds(), img, image.Point{}, draw.Src)

		c := freetype.NewContext()
		c.SetDPI(config.Label.Dpi)
		c.SetFont(font)
		c.SetFontSize(config.Label.Size)
		c.SetClip(rgba.Bounds())
		c.SetDst(rgba)
		c.SetSrc(fontForeground)

		// Draw the text
		_, err = c.DrawString(numberString, freetype.Pt(config.Label.PositionX, config.Label.PositionY))
		if err != nil {
			log.Println(err)
			return
		}

		var targetFile, err = os.Create(targetFileName)
		defer targetFile.Close()
		if err != nil {
			fmt.Println(err)
		}
		jpeg.Encode(targetFile, rgba, nil)
		var bytes, _ = ioutil.ReadFile(targetFileName)

		url, err := UploadBytesToBlob(storageAccountKey, bytes, numberString+".jpeg")
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("Upload completed: " + url)
	}
}
