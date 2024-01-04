package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	id := GetID()
	t, taskID, fileNames, nReduce := GetTask(id)

	if t == "Map" {
		res := MapPlayer(mapf, fileNames[0])

		outputFileNames := StoreKV(taskID, res, nReduce)
		if len(outputFileNames) != 0 {
			DoneMap(taskID, outputFileNames)
		}
	} else {
		ReducerPlayer(reducef, taskID, fileNames)
		DoneReduce(taskID)
	}
}

func GetID() int {
	args := EmptyArgs{}

	reply := GetIDReply{}

	ok := call("Coordinator.GetID", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Id %v\n", reply.Id)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.Id
}

func GetTask(id int) (string, int, []string, int) {
	args := GetTaskArgs{
		id,
	}

	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)

	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.T %v fileNames %v\n", reply.T, reply.FileNames)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply.T, reply.TaskId, reply.FileNames, reply.NReduce
}

func MapPlayer(mapf func(string, string) []KeyValue, filename string) []KeyValue {
	// intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return mapf(filename, string(content))
}

func StoreKV(id int, res []KeyValue, nReduce int) []string {
	outputFileNames := []string{}

	encs := []*json.Encoder{}
	for i := 0; i < nReduce; i++ {
		filename := "intermediate/mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i)
		outputFileNames = append(outputFileNames, filename)
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return []string{}
		}
		encs = append(encs, json.NewEncoder(file))
	}

	for _, kv := range res {
		index := ihash(kv.Key) % nReduce
		enc := encs[index]
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot failed into %v", outputFileNames[index])
			return []string{}
		}
	}

	return outputFileNames
}

func DoneMap(id int, outputFileNames []string) {
	args := DoneMapArgs{
		id,
		outputFileNames,
	}

	reply := EmptyReply{}

	ok := call("Coordinator.DoneMap", &args, &reply)

	if ok {
		fmt.Printf("Map Task %v Come Over\n", id)
	} else {
		fmt.Printf("DoneMap call failed!\n")
	}
}

func ReducerPlayer(reducef func(string, []string) string, id int, filenames []string) {
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	oname := "result/mr-out-" + strconv.Itoa(id)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func DoneReduce(id int) {
	args := DoneReduceArgs{
		id,
	}

	reply := EmptyReply{}

	ok := call("Coordinator.DoneReduce", &args, &reply)

	if ok {
		fmt.Printf("Reduce Task %v Come Over\n", id)
	} else {
		fmt.Printf("DoneReduce call failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
