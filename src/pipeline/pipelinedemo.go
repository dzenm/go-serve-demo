package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"time"
)

const (
	fOutName = "large.out"
	fInName  = "large.in"
	nLarge   = 100000000
	nSmall   = 64
	i        = iota
	c = iota
)

var startTime time.Time

/**
 * 初始化
 */
func Init() {
	startTime = time.Now()
}

func main() {
	// FileDemo(fInName, nLarge)
	// SortDemo()
	externalsort()
}

/**
 *	完整外部排序
 */
func externalsort() {
	source := createPipeline(fInName, 800000000, 4)
	writeToFile(source, fOutName)
	printFile(fOutName)
}

/**
 * 文件排序
 */
func FileDemo(fileName string, n int) {
	// 生成文件，并存入随机数据
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	source := RandomSource(n)
	writer := bufio.NewWriter(file)
	WriterSink(writer, source)
	writer.Flush()

	file, err = os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	source = ReaderSource(bufio.NewReader(file), -1)
	count := 0
	for v := range source {
		fmt.Println(v)
		count ++
		if count >= 100 {
			break
		}
	}
}

/**
 * 外部排序测试
 */
func SortDemo() {
	p := Merge(
		InMemSort(ArraySource(3, 10, 8, 6, 9, 18, 16, 76, 25)),
		InMemSort(ArraySource(56, 20, 48, 16, 99, 78, 11, 22, 45)))
	// 输出排序结果
	for {
		if num, ok := <-p; ok {
			fmt.Print(num, " ")
		} else {
			break
		}
	}
}

// 读取数据
func ArraySource(nums ...int) chan int {
	out := make(chan int)
	go func() {
		for _, v := range nums {
			out <- v
		}
		close(out)
	}()
	return out
}

// 内部排序
func InMemSort(in <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		// Read into memory
		num := [] int{}
		for v := range in {
			num = append(num, v)
		}
		fmt.Println("Read done: ", time.Now().Sub(startTime))
		// Sort
		sort.Ints(num)
		fmt.Println("InMemSort done: ", time.Now().Sub(startTime))

		// Output
		for _, v := range num {
			out <- v
		}
		close(out)
	}()
	return out
}

// 归并
func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		v1, ok1 := <-in1
		v2, ok2 := <-in2
		for ok1 || ok2 {
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				v1, ok1 = <-in1
			} else {
				out <- v2
				v2, ok2 = <-in2
			}
		}
		close(out)
		fmt.Println("Merge done: ", time.Now().Sub(startTime))
	}()
	return out
}

// 两两归并
func MergeN(inputs ...<-chan int) <-chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}
	m := len(inputs) / 2
	// merge inputs[0...m] and inputs[m...]
	return Merge(MergeN(inputs[:m]...), MergeN(inputs[m:]...))
}

// 读取数据源
func ReaderSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		buffer := make([]byte, 8)
		bytesRead := 0
		for {
			n, err := reader.Read(buffer)
			bytesRead += n
			if n > 0 {
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
			if err != nil || (chunkSize != -1 && bytesRead >= chunkSize) {
				break
			}
		}
		close(out)
	}()
	return out
}

// 写入数据
func WriterSink(writer io.Writer, in <-chan int) {
	for v := range in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(v))
		writer.Write(buffer)
	}
}

// 生成随机数据源
func RandomSource(count int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

// 创建排序
func createPipeline(fileName string, fileSize, chunkCount int) <-chan int {
	chunkSize := fileSize / chunkCount
	Init()
	sortResults := []<-chan int{} // 收集Merge的结果
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		file.Seek(int64(i*chunkSize), 0)
		source := ReaderSource(bufio.NewReader(file), chunkSize)
		sortResults = append(sortResults, InMemSort(source))
	}
	return MergeN(sortResults...)
}

// 写入文件
func writeToFile(source <-chan int, fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	defer writer.Flush()
	WriterSink(writer, source)
}

// 输出文件
func printFile(fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	p := ReaderSource(bufio.NewReader(file), -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count > 10 {
			break
		}
	}
}
