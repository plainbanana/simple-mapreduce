package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

type kvInput struct {
	Key   []int
	Value []string
}

type kvMap struct {
	Key   []string
	Value []int
}

type kvPreReduced struct {
	Key   []string
	Value [][]int
}

func main() {
	envLoad()

	// init dir
	if err := os.RemoveAll(os.Getenv("MAP_SAVE_DIR")); err != nil {
		fmt.Println(err)
	}
	if err := os.MkdirAll(os.Getenv("MAP_SAVE_DIR"), 0777); err != nil {
		fmt.Println(err)
	}

	// open file for input
	file, err := os.Open(os.Getenv("MAP_INPUTPASS"))
	if err != nil {
		log.Fatal("Error:", err)
	}
	defer file.Close()

	// read each line of file
	str := []string{}
	reader := bufio.NewReaderSize(file, 4096)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal("Error:", err)
		} else if string(line) == "" {
			continue
		}
		str = append(str, string(line))
	}

	s := kvInput{} // input struct for myMap function
	n := 0         // document id
	for {
		s.Key = append(s.Key, int(n))
		s.Value = append(s.Value, string(str[n]))
		n++
		if n == len(str) {
			break
		}
	}

	// exec myMap parallel
	var wg sync.WaitGroup
	processNum, _ := strconv.Atoi(os.Getenv("NUM_MAP"))
	semaphore := make(chan int, processNum)
	for i := range s.Key {
		wg.Add(1)
		sk := s.Key[i]
		sv := s.Value[i]
		go func(i int, sk int, sv string) {
			defer wg.Done()
			semaphore <- 1
			myMap(sk, sv) // myMap function
			<-semaphore
			log.Println("status : added!!")
		}(i, sk, sv)
	}
	wg.Wait()
	close(semaphore)

	// myReduce function
	// inside this func, exec reduce task parallel
	myReduce()
}

func envLoad() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error:", err)
	}
}

// calc hash and mod MAP_NUM_REDUCE to do hash_shuffle
// 16^2 pattren
func hashMod(value string) int {
	str := fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
	numHash, err := strconv.ParseInt(str[:2], 16, 10)
	if err != nil {
		log.Fatal("Error:", err)
	}
	numReduce, err := strconv.Atoi(os.Getenv("NUM_REDUCE"))
	if err != nil {
		log.Fatal("Error:", err)
	}
	fHash := float64(numHash)
	fReduce := float64(numReduce)
	return int(math.Mod(fHash, fReduce))
}

func myMap(docid int, text string) {
	rand.Seed(time.Now().UnixNano())

	// start Map
	s := kvMap{}
	tmpStr := strings.Fields(text)
	for _, v := range tmpStr {
		s.Key = append(s.Key, v)
		// s.Key = append(s.Key, fmt.Sprintf("%x", sha256.Sum256([]byte(v))))
		s.Value = append(s.Value, 1) // for wordcount, set "1" each words
	}

	// start shuffle
	rand := strconv.Itoa(rand.Intn(100000000000))
	for i := 0; i <= len(s.Key)-1; i++ {
		f := hashMod(s.Key[i])
		filepass := os.Getenv("MAP_SAVE_DIR") + "/" + rand + ":" + strconv.Itoa(f)
		file, err := os.OpenFile(filepass, os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			log.Fatal("Error:", err)
		}
		defer file.Close()
		// save mapped key and value as gob-binary format
		enc := gob.NewEncoder(file)
		if err := enc.Encode(s); err != nil {
			log.Fatal(err)
		}
	}
	log.Println("status : finish this Map task!!!")
}

func myReduce() {
	str := os.Getenv("MAP_SAVE_DIR")
	numReduce, err := strconv.Atoi(os.Getenv("NUM_REDUCE"))
	if err != nil {
		log.Fatal("Error:", err)
	}
	var wg sync.WaitGroup
	semaphore := make(chan int, numReduce)
	for i := 0; i < numReduce; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			semaphore <- 1
			{
				files, err := filepath.Glob(str + "/*" + ":" + strconv.Itoa(i))
				if err != nil {
					log.Fatal("Error:", err)
				}
				s := kvMap{}
				for _, f := range files {
					file, err := os.Open(f)
					if err != nil {
						log.Fatal("Error:", err)
					}
					defer file.Close()
					tmp := kvMap{}
					dec := gob.NewDecoder(file)
					if err := dec.Decode(&tmp); err != nil {
						log.Fatal("decode error:", err)
					}
					for i := range tmp.Key {
						s.Key = append(s.Key, tmp.Key[i])
						s.Value = append(s.Value, tmp.Value[i])
					}
				}

				// get unique keys
				m := make(map[string]bool)
				uniq := []string{}
				for _, v := range s.Key {
					if !m[v] {
						m[v] = true
						uniq = append(uniq, v)
					}
				}

				// pre task
				// single key and multi val
				fmt.Println("AAAAAa")
				ss := kvPreReduced{} // reduced struct
				for _, v := range uniq {
					ss.Key = append(ss.Key, v)
					val := []int{}
					for i, vv := range s.Key {
						if vv == v {
							val = append(val, s.Value[i])
						}
					}
					ss.Value = append(ss.Value, val)
				}

				// reduce task
				reduced := kvMap{}
				for i, v := range ss.Key {
					reduced.Key = append(reduced.Key, v)
					num := 0
					for _, vv := range ss.Value[i] {
						num += vv
					}
					reduced.Value = append(reduced.Value, num)
				}

				// output result
				rand.Seed(time.Now().UnixNano())
				rand := strconv.Itoa(rand.Intn(100000000000))
				filepass := os.Getenv("MAP_SAVE_DIR") + "/" + "result:" + rand + ".txt"
				file, err := os.OpenFile(filepass, os.O_CREATE|os.O_WRONLY, 0777)
				if err != nil {
					log.Fatal("Error:", err)
				}
				defer file.Close()
				for i, v := range reduced.Key {
					fmt.Fprintln(file, v+":"+strconv.Itoa(reduced.Value[i]))
				}
				log.Println("status : output result file")
			}
			<-semaphore
		}(i)
	}
	wg.Wait()
	close(semaphore)
}
