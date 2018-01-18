package mapreduce

import "fmt"
import "sync"

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

func deriveWork(workerName string,jobName string,mapFiles []string,phase jobPhase,n_other int,c int,registerChan chan string,wg *sync.WaitGroup){
	if(call(workerName,"Worker.DoTask",DoTaskArgs{jobName,mapFiles[c],phase,c,n_other},nil)){
		wg.Done();
		registerChan<-workerName
	}else{
		workerName := <-registerChan
		deriveWork(workerName,jobName,mapFiles,phase,n_other,c,registerChan,wg)
	}
}

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	fmt.Printf("All of %d work\n",len(mapFiles))
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var wg sync.WaitGroup
	wg.Add(ntasks)
	for taskNum:=0;taskNum<ntasks;taskNum++ {
		workerName:= <-registerChan
		go func (c int){
			deriveWork(workerName,jobName,mapFiles,phase,n_other,c,registerChan,&wg)
		}(taskNum)
	}

	wg.Wait();
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
