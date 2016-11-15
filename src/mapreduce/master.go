package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
  Operation JobType
  JobNumber int
  Status bool
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func AssignJob(file string,other int, worker *WorkerInfo,WorkDone chan *WorkerInfo){
	args:=&DoJobArgs{file,worker.Operation,worker.JobNumber,other}
	var reply DoJobReply
	worker.Status=call(worker.address, "Worker.DoJob", args, &reply)
	worker.Status= worker.Status && reply.OK
	WorkDone <- worker 
}

func (mr *MapReduce) MakeWorkerAvailable(){
	select {
    		case available := <-mr.registerChannel:
        		//fmt.Println("Worker Joined: " + available)
			mr.availableWorkers.PushBack(available)
    		default:
        		//WAITING FOR WORKERS
    	}
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
	workCount:=0

	for i:=0;i<mr.nMap;i++{
		mr.mapJobs.PushBack(&WorkerInfo{"None",Map,i,false})
  	}

  	for i:=0;i<mr.nReduce;i++{
		mr.reduceJobs.PushBack(&WorkerInfo{"None",Reduce,i,false})
  	}

	for mr.mapJobs.Len() >0{
		mr.MakeWorkerAvailable()
		workCount=0
		WorkDone:= make(chan *WorkerInfo,mr.availableWorkers.Len())
		for mr.availableWorkers.Len() > 0 && mr.mapJobs.Len() >0{
			worker:= mr.availableWorkers.Front()
			job:=mr.mapJobs.Front()
			job.Value.(*WorkerInfo).address = worker.Value.(string)
			go AssignJob(mr.file,mr.nReduce,job.Value.(*WorkerInfo),WorkDone)
			mr.availableWorkers.Remove(worker)
			mr.mapJobs.Remove(job)
			workCount++
		}
		for i:=0;i<workCount;i++{
			job:= <- WorkDone
			if job.Status{
				mr.availableWorkers.PushBack(job.address)
			}else{
				mr.mapJobs.PushBack(job)
			}
		}
	}

	for mr.reduceJobs.Len() >0{
		mr.MakeWorkerAvailable()
		workCount=0
		WorkDone:= make(chan *WorkerInfo,mr.availableWorkers.Len())
		for mr.availableWorkers.Len() > 0 && mr.reduceJobs.Len() >0{
			worker:= mr.availableWorkers.Front()
			job:=mr.reduceJobs.Front()
			job.Value.(*WorkerInfo).address = worker.Value.(string)
			go AssignJob(mr.file,mr.nMap,job.Value.(*WorkerInfo),WorkDone)
			mr.availableWorkers.Remove(worker)
			mr.reduceJobs.Remove(job)
			workCount++
		}
		for i:=0;i<workCount;i++{
			job:= <- WorkDone
			if job.Status{
				mr.availableWorkers.PushBack(job.address)
			}else{
				mr.reduceJobs.PushBack(job)
			}
		}
	}
	return mr.KillWorkers()
}
