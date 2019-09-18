package mapreduce

import (
	"bytes"
	"encoding/json"
	//"bufio"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	/*
	      Each call to doMap() reads the appropriate file, calls the map function
	   on that file's contents, and produces nReduce files for each map file.
	   Thus, after all map tasks are done, the total number of files will be the product
	   of the number of files given to map (nIn) and nReduce.
	*/

	/*

		1- Read file and store it as String Content
		2-  Feed the mapF with the file name and file Content
		3- mapF will do some unknown calculations that will convert content into Array of Key/Value. In this example
				Key would be the Content data row, value is nil (another function might make it a number like Count)
		4- Now we have an Array of Key/Value. We divide the array into nReduce parts and put each part into a file.
				Hence, we will have nReduce number of files
		5- To know which row goes to which file, we use iHash function. How? we feed it the Key value of the row which will
				outputs a number call it HashVal. We then use HashVal and do a Mod operation of nReduce to guarantee that the
				total array parts are exactly nReduce parts (Mod nReduce will seperate data into nReduce parts)
		6- We then save the nReduce files with a specific naming convention which can be found using ReduceName function
			reduceName(jobName, mapTaskNumber, nReduce) which will save the data into only nReduce # of files (make sure to append
			not overwite). We don't save KV rows yet. see 7
		7- Now we have nReduce # of files to feed to Reduce phase. We now need to make transofrm data format to be used
			properly in next stage. We need to convert the KeyValue data struct to JSON. How? we write out
			a data structure as a JSON string to a file using the commented code below. The corresponding
			decoding functions can be found in common_reduce.go.
			****
			enc := json.NewEncoder(file)
			for _, kv := ... {
			err := enc.Encode(&kv)
			****
			So after when we save Array parts to the designated output files, we save them as JSNON string using code above
		8- NOTE: we must be careful with Key and Value splitting or reading as it might contain Newlines or quotos ..etc as below
				**Coming up with scheme for how to store the key/value pairs on disk can be tricky,
				**especially when taking into account that both keys and values could contain
				**newlines, quotes, and any other character you can think of.
	*/

	dat, err := ioutil.ReadFile(inFile)
	//check(err)
	if err != nil {
		panic(err)
	}

	//** Step 1: reading from inFile to string
	contentFile := string(dat)
	//fmt.Println("file string ", (contentFile))

	//** Step 2: using maF to convert the file content to array of keyValue
	var kv []KeyValue
	kv = mapF(inFile, contentFile)

	//	fmt.Println("input rows: " , kv)

	//** Step 3, 4, 5 and 6 (write each part to specific file based on the iHash function

	for i := 0; i < nReduce; i++ {
		buffer := new(bytes.Buffer)
		enc := json.NewEncoder(buffer)
		var iHashMod int

		for j := 0; j < len(kv); j++ {
			myModD := uint32(nReduce)
			ihashV := ihash(kv[j].Key)
			iHashMod = int(ihashV % myModD)
			//	fmt.Println("ihash mod", iHashMod)
			if iHashMod == i {
				enc.Encode(kv[j])
				//		fmt.Println("encoding" , kv[j])
			}

		}
		filename := reduceName(jobName, mapTaskNumber, i)
		f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		f.Write(buffer.Bytes())
		f.Close()

	}

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
