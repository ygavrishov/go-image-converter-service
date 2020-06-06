package main

import (
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"io/ioutil"
	"os"
	"sync"

	"github.com/golang/freetype"
)

type serviceTask struct {
	Index          int
	SourceFilePath string
}

func main() {
	config, err := loadConfig()
	if err != nil {
		panic(err)
	}

	var fontBytes, _ = ioutil.ReadFile(config.Label.Fontfile)
	font, err := freetype.ParseFont(fontBytes)
	if err != nil {
		panic(err)
	}
	fontForeground := image.Black

	var storageAccountKey = azureBlobKeys{
		AccessKey:     os.Getenv("AZURE_ACCESS_KEY"),
		AccountName:   os.Getenv("AZURE_STORAGE_ACCOUNT"),
		ContainerName: config.Destination.BlobStorage.ContainerName,
	}

	files, _ := ioutil.ReadDir(config.Source.FolderPath)

	var tasks = make(chan serviceTask)
	var wg sync.WaitGroup

	for i := 0; i < config.NeededImageCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task := <-tasks
			var file, _ = os.Open(task.SourceFilePath)
			defer file.Close()
			var img, _, err = image.Decode(file)
			if err != nil {
				panic(err)
			}
			var numberString = fmt.Sprintf("%03d", task.Index)
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
				panic(err)
			}

			targetFile, err := os.Create(targetFileName)
			if err != nil {
				panic(err)
			}
			defer targetFile.Close()
			jpeg.Encode(targetFile, rgba, nil)
			var bytes, _ = ioutil.ReadFile(targetFileName)

			url, err := UploadBytesToBlob(storageAccountKey, bytes, numberString+".jpeg")
			if err != nil {
				panic(err)
			}
			fmt.Println("Upload completed: " + url)
		}()

	}

	var fileCounter = 0
	for i := 0; i < config.NeededImageCount; i++ {
		var fileInfo = files[fileCounter]
		if fileCounter < len(files)-1 {
			fileCounter++
		} else {
			fileCounter = 0
		}
		tasks <- serviceTask{
			Index:          i,
			SourceFilePath: config.Source.FolderPath + "\\" + fileInfo.Name(),
		}
	}

	wg.Wait()
}
