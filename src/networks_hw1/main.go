package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
)

type iperfoutput struct {
	start     start
	intervals string
	end       end
}
type start struct {
	test_start test_start
	sum_sent   sum_sent
}
type test_start struct {
	num_streams int
	blksize     int64
	omit        int
	duration    int
	bytes       int
	blocks      int
}
type end struct {
	streams                 string
	sum_sent                sum_sent
	sum_received            sum_received
	cpu_utilization_percent cpu_utilization_percent
}

type sum_sent struct {
	seconds         float32
	bytes           int64
	bits_per_second float32
}
type sum_received struct {
	seconds         float32
	bytes           int64
	bits_per_second float32
}

type cpu_utilization_percent struct {
	host_user   float32
	remote_user float32
}

func main() {

	/*
		sender - is iperf client, Upload speed from iperf client to iperf server is measured
		receiver - is iperf server, Download speed on iperf server from iperf client is measured
	*/

	//	ip1 := "iperf.biznetnetworks.com"//Indonesia
	//	ip2 := "10.254.145.232"//KAUST
	//	port2 :="-p 5200"
	ip3 := "134.209.227.237" //Germany
	//port3 := "-p 50001"
	verbose := "-V"
	//logfile := "-logfile"
	time := "-t" //--The time in seconds to transmit for. iPerf normally works by repeatedly sending an array of len bytes for time seconds. Default is 10 seconds.
	timeD :="20"
	parallel := "-P"// --The number of simultaneous connections to make to the server. Default is 1.
	streams :=1
	jsonoutput := "-J"
//	window := "-w" //The TCP window size is the amount of data that can be buffered during a connection without a validation from the receiver.
					//It can be between 2 and 65,535 bytes.TCP window size: 8.00 KByte (default)
//	windowD :="8000"
//	mss := "-M"
//	noDelay := "-N"
//	zeroCopy := "-Z"
	omit := "-O" //-- Omit the first n seconds of the test, to skip past the TCP TCP slowstart period.
//	omitD :="0"
//	length := "-l"//--length of buffer to read or write Default is 128 KB for TCP,
//	lD :="8000"

	//131072 byte blocks,
	//	cmdArgs := []string{"-c", ip3, time + strconv.Itoa(5), verbose, jsonoutput}
	//	iperf(cmdArgs)

//	appendToCSVNewRow([]string{"hi", "hoho"})
//	appendToCSVNewRow([]string{"hi2", "hoho2"})
//	cmdArgs1 := []string{"-c", ip2 , time + timeD, parallel + strconv.Itoa(streams), verbose, jsonoutput,omit+omit}

//	iperf(cmdArgs1, streams)

	//os.Exit(1)

//writeToCSV()
	for i := 0; i < 20; i++{
		println("running loop number " , i)
		cmdArgs1 := []string{"-c", ip3 , time + timeD, parallel + strconv.Itoa(1), verbose, jsonoutput,omit+omit }
		iperf(cmdArgs1, streams)

		/*
				println("running loop number " , i)

				cmdArgs1 := []string{"-c", ip1, time + timeD, parallel + strconv.Itoa(streams), verbose, jsonoutput,omit+omitD,
					length+lD, window+windowD }
				cmdArgs2 := []string{"-c", ip1, time + timeD, parallel + strconv.Itoa(streams), verbose, jsonoutput,omit+omitD,
					length+lD }
				cmdArgs3 := []string{"-c", ip1, time + timeD, parallel + strconv.Itoa(streams), verbose, jsonoutput,omit+omitD,
					length+lD, window+windowD }
				cmdArgs4 := []string{"-c", ip1, time + timeD, parallel + strconv.Itoa(streams), verbose, jsonoutput,omit+omitD,
					 window+windowD }
				cmdArgs5 := []string{"-c", ip1, time + timeD, parallel + strconv.Itoa(streams), verbose, jsonoutput,omit+omit}

				cmdArgs2 := []string{"-c", ip3, time + "10", verbose, jsonoutput, parallel + strconv.Itoa(streams),noDelay }
				cmdArgs3 := []string{"-c", ip3, time + "10", verbose, jsonoutput, parallel + strconv.Itoa(streams), zeroCopy}
				cmdArgs4 := []string{"-c", ip3, time + "10", verbose, jsonoutput, parallel + strconv.Itoa(streams), omit+"5"}
				cmdArgs5 := []string{"-c", ip3, time + "10", verbose, jsonoutput, parallel + strconv.Itoa(streams), window+"1000"}
				cmdArgs6 := []string{"-c", ip3, time + "10", verbose, jsonoutput, parallel + strconv.Itoa(streams), length+"1000"}

				println("running cmdArgs 1")
				iperf(cmdArgs1, streams)

				println("running cmdArgs 2")
				iperf(cmdArgs2, streams)

				println("running cmdArgs 3")
				iperf(cmdArgs3, streams)

				println("running cmdArgs 4")
				iperf(cmdArgs4, streams)

				println("running cmdArgs 5")
				iperf(cmdArgs5, streams)

				println("running cmdArgs 6")
				iperf(cmdArgs6, streams)*/
		println("done")

	}


	/*	for i := 2; i < 40; {
			cmdArgs := []string{"-c", ip3, time + "5", verbose, jsonoutput, parallel + strconv.Itoa(i)}
			iperf(cmdArgs, i,cmdArgs)
			i = i + 10
			println("i", i)
		}
	*/
}
func iperf(cmdArgs []string, streams int) {
	cmd := exec.Command("iperf3", cmdArgs...)
	//	cmd.Stdout = os.Stdout
	//	cmd.Stderr = os.Stderr
	stdoutStderr, err := cmd.CombinedOutput()
//	stdoutStderr = stdoutStderr
	if err != nil {
		// TODO: handle error more gracefully
		log.Fatal(err)
		println(err)
	}

	println(stdoutStderr)
	processOutput(stdoutStderr, streams, cmdArgs)

}
func processOutput(stdoutStderr []byte, streams int, args []string) bool {

	var result map[string]interface{}
	json.Unmarshal(stdoutStderr, &result)

	iPerobjectEnd := result["end"].(map[string]interface{})

	sum_sent := iPerobjectEnd["sum_sent"].(map[string]interface{})
	var sent_bits_per_second float64
	var sent_seconds float64
	var sent_bytes float64

	sum_received := iPerobjectEnd["sum_received"].(map[string]interface{})
	var r_bits_per_second float64
	var r_seconds float64
	var r_bytes float64

	cpu_utilization_percent := iPerobjectEnd["cpu_utilization_percent"].(map[string]interface{})
	var host_total float64
	var remote_total float64

	for key, value := range sum_sent {
		if key == "bits_per_second" {
			sent_bits_per_second = value.(float64)
		} else if key == "seconds" {
			sent_seconds = value.(float64)
		} else if key == "bytes" {
			sent_bytes = value.(float64)
		}
	}
	host_total = host_total
	remote_total = remote_total
	r_seconds = r_seconds
	sent_seconds = sent_seconds

	for key, value := range sum_received {
		if key == "bits_per_second" {
			r_bits_per_second = value.(float64)
		} else if key == "seconds" {
			r_seconds = value.(float64)
		} else if key == "bytes" {
			r_bytes = value.(float64)
		}
	}

	for key, value := range cpu_utilization_percent {
		if key == "host_total" {
			host_total = value.(float64)
		} else if key == "remote_total" {
			remote_total = value.(float64)
		}
	}

	dataLoss := (sent_bytes - r_bytes) / 1000000
	dataLossPercentage := (1-(r_bytes/sent_bytes))
	throughput := ((sent_bits_per_second + r_bits_per_second) / 2) / 1000000

	/*	println("sent seconds", int(sent_seconds))
		println("sent_bits_per_second", float32(sent_bits_per_second))
		println("sent bytes", int64(sent_bytes))

		println("r seconds", int(r_seconds))
		println("r bits_per_second", float32(r_bits_per_second))
		println("r bytes", int64(r_bytes))

		println("host_total", float32(host_total))
		println("remote_total", float32(remote_total))
		println("data Loss", float32(dataLoss) ,"Mbytes")
		println("throughput", ((throughput)), "Mbits/second")
	*/
	throughputStr := fmt.Sprintf("%.2f", throughput)
	lossStr := fmt.Sprintf("%f", dataLoss)
	hostStr := fmt.Sprintf("%.4f", host_total)
	remoteStr := fmt.Sprintf("%.4f", remote_total)
	dataLossPercentageStr := fmt.Sprintf("%.4f", dataLossPercentage)


	println(throughputStr, "Mbits/second")

	for i:=0;i<len(args) ;i++  {
		args[i] = "'"+args[i]
	}
	data := []string{throughputStr, lossStr, dataLossPercentageStr, hostStr, remoteStr,  strconv.Itoa(streams)}
	location := "Germany"
	connectionType := "Wireless"
	data = append(data, []string{location, connectionType}...)
	data = append(data, args...)

	appendToCSVNewRow(data)


	return true
}

func writeToCSV() {

	file, err := os.OpenFile("test.csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	defer file.Close()

	if err != nil {
		os.Exit(1)
	}

	x := []string{"Country", "City", "Population"}
	y := []string{"Japan", "Tokyo", "923456"}
	z := []string{"Australia", "Sydney", "789650"}
	csvWriter := csv.NewWriter(file)
	strWrite := [][]string{x, y, z}
	csvWriter.WriteAll(strWrite)
	csvWriter.Flush()
}

func appendToCSVNewRow(data []string) {

	file, err := os.OpenFile("runs.csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	defer file.Close()

	if err != nil {
		os.Exit(1)
	}

	csvWriter := csv.NewWriter(file)
	csvWriter.Write(data)
	csvWriter.Flush()
}
