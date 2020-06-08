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
	gender string
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
	[]personInfo{personInfo{gender: "m", age: 33}},
	[]personInfo{personInfo{gender: "m", age: 41}},
	[]personInfo{personInfo{gender: "f", age: 25}},
	[]personInfo{personInfo{gender: "f", age: 17}},
	[]personInfo{personInfo{gender: "m", age: 13}},
	[]personInfo{personInfo{gender: "m", age: 27}},
	[]personInfo{personInfo{gender: "m", age: 23}},
	[]personInfo{personInfo{gender: "m", age: 18}},
	[]personInfo{personInfo{gender: "m", age: 26}, personInfo{gender: "f", age: 27}},
	[]personInfo{personInfo{gender: "f", age: 21}}}
var streamCount = 100

func main() {
	//connect to aerospike
	client, err := aero.NewClient("localhost", 3000)
	panicOnError(err)
	fmt.Printf("Connected\n")

	var tasksForFrameExtractor = make(chan int)
	var tasksForFaceDetector = make(chan frameInfo)
	var tasksForExLevelDetector = make(chan frameInfo)
	var tasksForAgeDetector = make(chan facesInfo)
	var tasksForGenderDetector = make(chan facesInfo)

	var wgFrameExtractor sync.WaitGroup
	var wgFaceDetector sync.WaitGroup
	wgFaceDetector.Add(1)

	var wgExLevelDetector sync.WaitGroup
	wgExLevelDetector.Add(1)

	var wgAgeDetector sync.WaitGroup
	wgAgeDetector.Add(1)

	var wgGenderDetector sync.WaitGroup
	wgGenderDetector.Add(1)

	for i := 0; i < streamCount; i++ {
		wgFrameExtractor.Add(1)
		go func() {
			defer wgFrameExtractor.Done()
			streamID := <-tasksForFrameExtractor
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
		}()
	}

	go func() {
		defer wgFaceDetector.Done()
		counter := 0
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
			counter++
		}
		fmt.Printf("%d frames were processed in Face Detector\n", counter)
		close(tasksForAgeDetector)
		close(tasksForGenderDetector)
	}()

	go func() {
		defer wgExLevelDetector.Done()
		counter := 0
		for task := range tasksForExLevelDetector {
			//detect excitement level
			exLevel := task.streamID % 10

			//save data in the database
			key, err := aero.NewKey("test", "search-engine", task.key)
			panicOnError(err)
			bin := aero.NewBin("exLevel", exLevel)
			client.PutBins(nil, key, bin)
			panicOnError(err)
			counter++
		}
		fmt.Printf("%d frames were processed in Excitement Level Detector\n", counter)
	}()

	go func() {
		defer wgAgeDetector.Done()
		counter := 0
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
			counter++
		}
		fmt.Printf("%d frames were processed in Age Detector\n", counter)
	}()

	go func() {
		defer wgGenderDetector.Done()
		counter := 0
		for task := range tasksForGenderDetector {
			//detect gender
			staticFrame := staticFrames[task.streamID%10]
			gendersMap := make(map[string]int, len(staticFrame))
			for i := 0; i < len(task.faces); i++ {
				gendersMap[task.faces[i]] = staticFrame[i].age
			}

			//save data in the database
			key, err := aero.NewKey("test", "search-engine", task.key)
			panicOnError(err)
			bin := aero.NewBin("genders", gendersMap)
			client.PutBins(nil, key, bin)
			panicOnError(err)
			counter++
		}
		fmt.Printf("%d frames were processed in Gender Detector\n", counter)
	}()

	for i := 0; i < streamCount; i++ {
		tasksForFrameExtractor <- i
	}

	wgFrameExtractor.Wait()
	close(tasksForFaceDetector)
	close(tasksForExLevelDetector)
	wgFaceDetector.Wait()
	wgExLevelDetector.Wait()
	wgAgeDetector.Wait()
	wgGenderDetector.Wait()
}
