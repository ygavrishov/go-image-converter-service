package main

import (
	"fmt"
	"os"
	"os/exec"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/viper"
)

type awsLocation struct {
	Bucket  string
	BlobKey string
}
type imageSize struct {
	Width  int32
	Height int32
}
type scConfig struct {
	Source           awsLocation
	Destination      awsLocation
	WorkingFolder    string
	FfmpegPath       string
	OutputResolution imageSize
}

func loadConfig() (*scConfig, error) {
	viper.SetConfigFile("config.yaml")
	var config = scConfig{}
	var err = viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	viper.Unmarshal(&config)
	return &config, nil
}

func createAwsSession() (*session.Session, error) {
	var awsRegion = os.Getenv("AWS_REGION")
	var awsKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	var awsSecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsKeyID, awsSecretKey, ""),
	})
	return sess, err
}

func downloadFileFromS3(awsSession *session.Session, source *awsLocation, filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	var downloader = s3manager.NewDownloader(awsSession)
	_, err = downloader.Download(f, &s3.GetObjectInput{
		Key:    aws.String(source.BlobKey),
		Bucket: aws.String(source.Bucket),
	})
	if err != nil {
		return err
	}
	return nil
}

func uploadFileToS3(awsSession *session.Session, destination *awsLocation, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	var uploader = s3manager.NewUploader(awsSession)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Key:    aws.String(destination.BlobKey),
		Bucket: aws.String(destination.Bucket),
		Body:   f,
	})
	if err != nil {
		return err
	}
	return nil
}

func encodeImage(inputFilePath string, outputFilePath string, config *scConfig) error {
	var resolution = fmt.Sprintf("scale=%d:%d", config.OutputResolution.Width, config.OutputResolution.Height)
	var cmd = exec.Command(config.FfmpegPath, "-y", "-i", inputFilePath, "-vf", resolution, outputFilePath)
	err := cmd.Run()
	return err

}

func main() {
	config, err := loadConfig()
	if err != nil {
		panic(err)
	}
	var awsSession *session.Session
	awsSession, err = createAwsSession()
	if err != nil {
		panic(err)
	}

	var sourceFilePath = path.Join(config.WorkingFolder, path.Base(config.Source.BlobKey))
	var outputFilePath = path.Join(config.WorkingFolder, "outputfile.png")

	fmt.Println("Start download", config.Source.BlobKey, "...")
	err = downloadFileFromS3(awsSession, &config.Source, sourceFilePath)
	if err != nil {
		panic(err)
	}
	fmt.Println("File has been downloaded.")

	fmt.Println("Start encoding...")
	err = encodeImage(sourceFilePath, outputFilePath, config)
	if err != nil {
		panic(err)
	}
	fmt.Println("Encoding has been done successfully.")

	fmt.Println("Start upload to", config.Destination.BlobKey, "...")
	err = uploadFileToS3(awsSession, &config.Destination, outputFilePath)
	if err != nil {
		panic(err)
	}
	fmt.Println("File has been successfully uploaded.")

}
