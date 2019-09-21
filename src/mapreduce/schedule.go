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

	debug("________Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

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

	taskDoneChannel := make(chan bool, ntasks)

	worker := <-mr.registerChannel

	for i := 0; i < ntasks; i++ {

		fmt.Println("doing this i", i, "with worker ", worker, "for task", phase)
		taskArgs := new(DoTaskArgs)
		taskArgs.File = mr.files[i]
		taskArgs.JobName = mr.jobName
		taskArgs.Phase = phase
		taskArgs.TaskNumber = i
		taskArgs.NumOtherPhase = nios
		//	taskDoneChannel <- true
		go mr.yalla(worker, taskArgs, taskDoneChannel)
		worker = <-mr.registerChannel

	}

	go func() {
		mr.registerChannel <- worker
	}()

}

func (mr *Master) yalla(workerName string, taskArgs *DoTaskArgs, taskDone chan bool) {

	call(workerName, "Worker.DoTask", taskArgs, new(struct{}))
	fmt.Println("******* This task is done", taskArgs.TaskNumber, taskArgs.File, " by ", workerName)

	//	<-taskDone
	mr.registerChannel <- workerName

}
