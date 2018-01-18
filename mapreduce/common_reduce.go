package mapreduce

import (
	"strings"
	"os"
	"encoding/json"
	"io/ioutil"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
type KVSlice []KeyValue

func (p KVSlice) Len() int{
	return len(p)
}
func (p KVSlice) Swap(i,j int){
	temp := p[i]
	p[i] = p[j]
	p[j] = temp
}
func (p KVSlice) Less(i,j int) bool{
	return p[i].Key<p[j].Key
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	oFile,_ := os.Create(outFile)
	enc :=json.NewEncoder(oFile)

	var kvSlice KVSlice
	
	for i :=0;i<nMap;i++ {
		file,_ := ioutil.ReadFile(reduceName(jobName,i,reduceTaskNumber))
		reader := strings.NewReader(string(file))
		jsonDecode :=json.NewDecoder(reader)
		for jsonDecode.More() {
			var kv KeyValue
			jsonDecode.Decode(&kv)
			kvSlice = append(kvSlice,kv)
		}
	}
	if(kvSlice.Len()==0){
		return
	}

	sort.Sort(kvSlice)
	var tempSlice []string
	curKey := kvSlice[0].Key

	for _,kv := range kvSlice {
		if(kv.Key>curKey){
			enc.Encode(KeyValue{curKey,reduceF(curKey,tempSlice)})
			tempSlice = make([]string,0)
			tempSlice = append(tempSlice,kv.Value)
			curKey = kv.Key
		}else{
			tempSlice = append(tempSlice,kv.Value)
		}
	}
	enc.Encode(KeyValue{curKey,reduceF(curKey,tempSlice)})

}
