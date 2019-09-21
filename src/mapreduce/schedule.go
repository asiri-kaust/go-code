package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("___ _____Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	fmt.Printf("________Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	/*
		// All ntasks tasks have to be scheduled on workers, and only once all of
		// them have been completed successfully should the function return.
		// Remember that workers may fail, and that any given worker may finish
		// multiple tasks.
		//
		// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
		//

	*/

	pendingTasksChannel := make(chan bool, ntasks)

	worker := <-mr.registerChannel

	for i := 0; i < ntasks; i++ {

		//fmt.Println("doing this i", i, "with worker ", worker, "for task", phase)
		taskArgs := new(DoTaskArgs)
		taskArgs.File = mr.files[i]
		taskArgs.JobName = mr.jobName
		taskArgs.Phase = phase
		taskArgs.TaskNumber = i
		taskArgs.NumOtherPhase = nios

		pendingTasksChannel <- true
		go mr.yalla(worker, taskArgs, pendingTasksChannel)

		worker = <-mr.registerChannel
		for worker == "fail" {
			worker = <-mr.registerChannel
			if worker != "fail" {
				go mr.yalla(worker, taskArgs, pendingTasksChannel)
				worker = <-mr.registerChannel
			}
		}

		//	fmt.Println("$$$",len(pendingTasksChannel))
	}

	go func() {
		mr.registerChannel <- worker
	}()

	//	fmt.Println("### LEN " , len(pendingTasksChannel))

	/*for{
		fmt.Println("###222 LEN " , len(pendingTasksChannel))

		if(len(pendingTasksChannel)<1) {
			break
		}
	}*/

}

func (mr *Master) yalla(workerName string, taskArgs *DoTaskArgs, taskDone chan bool) bool {

	var result bool
	result = call(workerName, "Worker.DoTask", taskArgs, new(struct{}))

	if result {
		<-taskDone
		mr.registerChannel <- workerName
		return result
	} else {
		//	fmt.Println("******* This task ", taskArgs.TaskNumber, " by ", workerName , "is" , result, " file " , taskArgs.File)
		mr.registerChannel <- "fail"
		return result
	}

}
