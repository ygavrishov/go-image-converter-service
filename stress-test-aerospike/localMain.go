package main

import (
	"fmt"
	"sync"
	"time"

	aero "github.com/aerospike/aerospike-client-go"
	guid "github.com/beevik/guid"
)

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

type personInfo struct {
	age    int
	gender int
}

type frameInfo struct {
	streamID int
	time     int64
	key      string
}

type facesInfo struct {
	streamID int
	time     int64
	key      string
	faces    []string
}

var staticFrames = [][]personInfo{
	[]personInfo{personInfo{gender: 1, age: 33}},
	[]personInfo{personInfo{gender: 1, age: 41}},
	[]personInfo{personInfo{gender: 2, age: 25}},
	[]personInfo{personInfo{gender: 2, age: 17}},
	[]personInfo{personInfo{gender: 1, age: 13}},
	[]personInfo{personInfo{gender: 1, age: 27}},
	[]personInfo{personInfo{gender: 1, age: 23}},
	[]personInfo{personInfo{gender: 1, age: 18}},
	[]personInfo{personInfo{gender: 1, age: 26}, personInfo{gender: 2, age: 27}},
	[]personInfo{personInfo{gender: 2, age: 21}}}
var streamCount = 1000
var eventID = 1

var insertedCount = 0
var ageProcessedCount = 0
var genderProcessedCount = 0
var exLevelProcessedCount = 0
var faceDetectedCount = 0

func main() {
	//connect to aerospike
	client, err := aero.NewClient("localhost", 3000)
	panicOnError(err)
	fmt.Printf("Connected\n")

	var tasksForFaceDetector = make(chan frameInfo)
	var tasksForExLevelDetector = make(chan frameInfo)
	var tasksForAgeDetector = make(chan facesInfo)
	var tasksForGenderDetector = make(chan facesInfo)

	var wgFaceDetector sync.WaitGroup
	wgFaceDetector.Add(1)

	var wgExLevelDetector sync.WaitGroup
	wgExLevelDetector.Add(1)

	var wgAgeDetector sync.WaitGroup
	wgAgeDetector.Add(1)

	var wgGenderDetector sync.WaitGroup
	wgGenderDetector.Add(1)

	go func() {
		defer wgFaceDetector.Done()
		//faceDetectedCount := 0
		for task := range tasksForFaceDetector {
			//detect faces
			staticFrame := staticFrames[task.streamID%10]
			faceIds := make([]string, len(staticFrame))
			for i := 0; i < len(staticFrame); i++ {
				faceIds[i] = guid.New().String()
			}

			//save data in the database
			key, err := aero.NewKey("test", "search-engine", task.key)
			panicOnError(err)
			bin := aero.NewBin("faceIds", faceIds)
			client.PutBins(nil, key, bin)
			panicOnError(err)

			faces := facesInfo{
				key:      task.key,
				streamID: task.streamID,
				time:     task.time,
				faces:    faceIds,
			}
			//send message to age detector
			tasksForAgeDetector <- faces
			//send message to gender detector
			tasksForGenderDetector <- faces
			faceDetectedCount++
		}
		fmt.Printf("%d frames were processed in Face Detector\n", faceDetectedCount)
		close(tasksForAgeDetector)
		close(tasksForGenderDetector)
	}()

	go func() {
		defer wgExLevelDetector.Done()
		//exLevelProcessedCount := 0
		for task := range tasksForExLevelDetector {
			//detect excitement level
			exLevel := task.streamID % 10

			//save data in the database
			key, err := aero.NewKey("test", "search-engine", task.key)
			panicOnError(err)
			bin := aero.NewBin("exLevel", exLevel)
			client.PutBins(nil, key, bin)
			panicOnError(err)
			exLevelProcessedCount++
		}
		fmt.Printf("%d frames were processed in Excitement Level Detector\n", exLevelProcessedCount)
	}()

	go func() {
		defer wgAgeDetector.Done()
		//ageProcessedCount := 0
		for task := range tasksForAgeDetector {
			//detect age
			staticFrame := staticFrames[task.streamID%10]
			agesMap := make(map[string]int, len(staticFrame))
			for i := 0; i < len(task.faces); i++ {
				agesMap[task.faces[i]] = staticFrame[i].age
			}

			//save data in the database
			key, err := aero.NewKey("test", "search-engine", task.key)
			panicOnError(err)
			bin := aero.NewBin("ages", agesMap)
			client.PutBins(nil, key, bin)
			panicOnError(err)
			ageProcessedCount++
		}
		fmt.Printf("%d frames were processed in Age Detector\n", ageProcessedCount)
	}()

	go func() {
		defer wgGenderDetector.Done()
		//genderProcessedCount := 0
		for task := range tasksForGenderDetector {
			//detect gender
			staticFrame := staticFrames[task.streamID%10]
			gendersMap := make(map[string]int, len(staticFrame))
			for i := 0; i < len(task.faces); i++ {
				gendersMap[task.faces[i]] = staticFrame[i].gender
			}

			//save data in the database
			key, err := aero.NewKey("test", "search-engine", task.key)
			panicOnError(err)
			bin := aero.NewBin("genders", gendersMap)
			client.PutBins(nil, key, bin)
			panicOnError(err)
			genderProcessedCount++
		}
		fmt.Printf("%d frames were processed in Gender Detector\n", genderProcessedCount)
	}()

	for {
		for streamID := 0; streamID < streamCount; streamID++ {
			thumbnailURL := fmt.Sprintf("https://15sofstorageqa02.blob.core.windows.net/sample-thumbnails/%03d.jpeg", streamID)

			ts := time.Now().Unix()
			rowKey := fmt.Sprintf("%d:%d", streamID, ts)

			key, err := aero.NewKey("test", "search-engine", rowKey)
			panicOnError(err)

			//save frame in the database
			bins := aero.BinMap{
				"key":          rowKey,
				"streamId":     streamID,
				"time":         ts,
				"thumbnailUrl": thumbnailURL,
				"eventId":      eventID,
			}
			writePolicy := aero.NewWritePolicy(0, 0)
			//writePolicy.Expiration = 2 // seconds
			err = client.Put(writePolicy, key, bins)
			panicOnError(err)

			frame := frameInfo{
				streamID: streamID,
				time:     ts,
				key:      rowKey,
			}
			//send message to face detector
			tasksForFaceDetector <- frame
			//send message to excitement level detector
			tasksForExLevelDetector <- frame
			insertedCount++
		}

		time.Sleep(1 * time.Second)
		fmt.Printf("%d frames were processed inserted\n", insertedCount)
		fmt.Printf("%d frames were processed in Excitement Level Detector\n", exLevelProcessedCount)
		fmt.Printf("%d frames were processed in Age Detector\n", ageProcessedCount)
		fmt.Printf("%d frames were processed in Gender Detector\n", genderProcessedCount)
		fmt.Printf("%d frames were processed in Face Detector\n", faceDetectedCount)
	}

	// close(tasksForExLevelDetector)
	// wgFaceDetector.Wait()
	// wgExLevelDetector.Wait()
	// wgAgeDetector.Wait()
	// wgGenderDetector.Wait()
}
