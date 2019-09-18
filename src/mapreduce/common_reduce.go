package mapreduce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValueArray struct {
	Key   string
	Value []string
}

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int,             // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	/*
		1- Read each Map output file from prev phase (there are nReduce files).
		2- Map Output file names can be calculated using the function reduceName(jobName, m, reduceTaskNumber) where m is nMap (nReduce)
		3- We then read from each file and contruct nReduce (nMap) number of KeyValue arrays
		3- When we read from each files, we must decode each row from JSON to KeyValue. If we used JSON we can
		decode  by creating a decoder, and then repeatedly calling .Decode() on it until Decode() returns an error.
		4- We now have nReduce number of decoded KeyValue arrays. Next step is sort each array by the Key
		5- We take each Array of KeyValue and feed each row to reduceF func(key string, values []string) string
		6- This will return a string for each row which will need to be converted to JSON encoded KeyValue object
		7- The JSON output of every operation should be saved into a file named mergeName(jobName, reduceTaskNumber) like the following
				// enc := json.NewEncoder(mergeFile)
				// for key in ... {
				// 	enc.Encode(KeyValue{key, reduceF(...)})
				// }
				// file.Close()
	*/

	var listOfStructs map[string][]string
	listOfStructs = make(map[string][]string)
	var keys []string

	for i := 0; i < nMap; i++ {

		/*
		   Step 1: read from file,  decode data and return list of pairs
		*/
		// read this file
		fileName := reduceName(jobName, i, reduceTaskNumber)
		//decode all values into list of structs

		f, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("readFile: %s", err)
		}

		dec := json.NewDecoder(f)
		for {
			//		fmt.Println("<<<<<<<STEP 1 For")

			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
				fmt.Printf("ERROR readFile: %s", err)
			}
			//	fmt.Println("<<<<<<<STEP 1.1 KV ", kv)

			//check if they key was added or not. If it is then append array
			_, ok := listOfStructs[kv.Key]
			if ok {
				listOfStructs[kv.Key] = append(listOfStructs[kv.Key], kv.Value)
			} else {
				listOfStructs[kv.Key] = []string{kv.Value}
				keys = append(keys, kv.Key)
			}

		}
	}

	/*
		Step 3: sort
	*/

	keysArray := make([]string, 0, len(listOfStructs))
	for k := range listOfStructs {
		keysArray = append(keysArray, k)
	}
	sort.Strings(keysArray)

	var sortedMapOfStructs map[string][]string
	sortedMapOfStructs = make(map[string][]string)

	for _, k := range keysArray {
		//fmt.Println("MAPS ", k, listOfStructs[k])
		sortedMapOfStructs[k] = listOfStructs[k]
	}

	listOfStructs = sortedMapOfStructs

	/*
	   step 4: Write reduceF output to disk
	*/

	buffer := new(bytes.Buffer)
	enc := json.NewEncoder(buffer)

	var lastOutput KeyValue
	for _, k := range keys {
		output := reduceF(k, listOfStructs[k])
		lastOutput.Key = k
		lastOutput.Value = output
		enc.Encode(lastOutput)
	}

	f, err := os.OpenFile(mergeName(jobName, reduceTaskNumber), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	f.Write(buffer.Bytes())

	f.Close()

	//DONE

}
