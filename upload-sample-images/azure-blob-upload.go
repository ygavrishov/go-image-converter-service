package main

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type azureBlobKeys struct {
	AccessKey     string
	AccountName   string
	ContainerName string
}

// GetAccountInfo ...
func GetAccountInfo() (string, string, string, string) {
	azrKey := os.Getenv("AZURE_ACCESS_KEY")
	azrBlobAccountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	azrPrimaryBlobServiceEndpoint := fmt.Sprintf("https://%s.blob.core.windows.net/", azrBlobAccountName)
	azrBlobContainer := os.Getenv("CONTAINER_NAME")
	fmt.Println("Container: " + azrBlobContainer)
	fmt.Println("azrPrimaryBlobServiceEndpoint: " + azrPrimaryBlobServiceEndpoint)
	fmt.Println("azrBlobAccountName: " + azrBlobAccountName)

	return azrKey, azrBlobAccountName, azrPrimaryBlobServiceEndpoint, azrBlobContainer
}

// UploadBytesToBlob ...
func UploadBytesToBlob(storageAccountKey azureBlobKeys, b []byte, blobName string) (string, error) {
	endPoint := fmt.Sprintf("https://%s.blob.core.windows.net/", storageAccountKey.AccountName)
	u, _ := url.Parse(fmt.Sprint(endPoint, storageAccountKey.ContainerName, "/", blobName))

	credential, errC := azblob.NewSharedKeyCredential(storageAccountKey.AccountName, storageAccountKey.AccessKey)
	if errC != nil {
		return "", errC
	}

	// Another Azure Specific object, which combines our generated URL and credentials
	blockBlobURL := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))

	ctx := context.Background()

	// Provide any needed options to UploadToBlockBlobOptions (https://godoc.org/github.com/Azure/azure-storage-blob-go/azblob#UploadToBlockBlobOptions)
	o := azblob.UploadToBlockBlobOptions{
		BlobHTTPHeaders: azblob.BlobHTTPHeaders{
			ContentType: "image/jpg", //  Add any needed headers here
		},
	}

	// Combine all the pieces and perform the upload using UploadBufferToBlockBlob (https://godoc.org/github.com/Azure/azure-storage-blob-go/azblob#UploadBufferToBlockBlob)
	_, errU := azblob.UploadBufferToBlockBlob(ctx, b, blockBlobURL, o)
	return blockBlobURL.String(), errU
}
