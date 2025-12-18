// Code made by Shamil Abdurahmanov 
// StudentID: 241ADB070
// TO RUN THE CODE USE THESE COMMANDS: go run . -r 20 (for random numbers)
// go run . -i input.txt (for numbers in the input.txt file)
// go run . -d incoming  (to Run directory mode)
//
// 
package main

import (
	"bufio"
	"container/heap"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Keep these in sync with the comment header above (used for -d output dir name)
const (
	StudentFirstName = "Shamil"
	StudentSurname   = "Abdurahmanov"
	StudentID        = "241ADB070"
)

func main() {
	rN := flag.Int("r", -1, "generate N random integers (N >= 10)")
	iFile := flag.String("i", "", "input file with one integer per line")
	dDir := flag.String("d", "", "directory containing .txt files to sort")
	flag.Parse()

	// exactly one mode
	modes := 0
	if *rN != -1 {
		modes++
	}
	if *iFile != "" {
		modes++
	}
	if *dDir != "" {
		modes++
	}
	if modes != 1 {
		log.Fatal("Error: choose exactly one mode: -r N OR -i file.txt OR -d dir")
	}

	switch {
	case *rN != -1:
		if err := runRandom(*rN); err != nil {
			log.Fatal(err)
		}
	case *iFile != "":
		if err := runInputFile(*iFile); err != nil {
			log.Fatal(err)
		}
	case *dDir != "":
		if err := runDirectory(*dDir); err != nil {
			log.Fatal(err)
		}
	}
}

// -----------------------------
// -r mode logic
// -----------------------------

func runRandom(n int) error {
	if n < 10 {
		return errors.New("Error: N must be >= 10")
	}
	numbers := generateRandomNumbers(n) // range documented in README / below

	fmt.Println("Original numbers (unsorted):")
	fmt.Println(numbers)

	chunks := splitIntoChunks(numbers)
	fmt.Println("\nChunks before sorting:")
	printChunks(chunks)

	sortChunksConcurrently(chunks)
	fmt.Println("\nChunks after sorting:")
	printChunks(chunks)

	result := mergeSortedChunks(chunks)
	fmt.Println("\nFinal merged sorted result:")
	fmt.Println(result)
	return nil
}

// -----------------------------
// -i mode logic
// -----------------------------

func runInputFile(path string) error {
	numbers, err := readIntsFromFile(path)
	if err != nil {
		return err
	}
	if len(numbers) < 10 {
		return errors.New("Error: fewer than 10 valid numbers in input file")
	}

	fmt.Println("Original numbers (unsorted):")
	fmt.Println(numbers)

	chunks := splitIntoChunks(numbers)
	fmt.Println("\nChunks before sorting:")
	printChunks(chunks)

	sortChunksConcurrently(chunks)
	fmt.Println("\nChunks after sorting:")
	printChunks(chunks)

	result := mergeSortedChunks(chunks)
	fmt.Println("\nFinal merged sorted result:")
	fmt.Println(result)
	return nil
}

// -----------------------------
// -d mode logic
// -----------------------------

func runDirectory(inputDir string) error {
	info, err := os.Stat(inputDir)
	if err != nil {
		return fmt.Errorf("Error: cannot access directory: %v", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("Error: %s is not a directory", inputDir)
	}

	parent := filepath.Dir(strings.TrimRight(inputDir, string(os.PathSeparator)))
	base := filepath.Base(strings.TrimRight(inputDir, string(os.PathSeparator)))

	outDirName := fmt.Sprintf("%s_sorted_%s_%s_%s",
		base,
		strings.ToLower(StudentFirstName),
		strings.ToLower(StudentSurname),
		StudentID,
	)
	outputDir := filepath.Join(parent, outDirName)
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("Error: cannot create output directory: %v", err)
	}

	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return fmt.Errorf("Error: cannot read directory: %v", err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.ToLower(filepath.Ext(name)) != ".txt" {
			continue // ignore non-.txt
		}

		inPath := filepath.Join(inputDir, name)
		nums, err := readIntsFromFile(inPath)
		if err != nil {
			return fmt.Errorf("Error in %s: %v", name, err)
		}
		if len(nums) < 10 {
			return fmt.Errorf("Error in %s: fewer than 10 valid numbers", name)
		}

		chunks := splitIntoChunks(nums)
		sortChunksConcurrently(chunks)
		merged := mergeSortedChunks(chunks)

		outPath := filepath.Join(outputDir, name)
		if err := writeIntsToFile(outPath, merged); err != nil {
			return fmt.Errorf("Error writing %s: %v", outPath, err)
		}
	}

	return nil
}

// -----------------------------
// Chunking logic
// -----------------------------

func numChunksFor(n int) int {
	k := int(math.Ceil(math.Sqrt(float64(n))))
	if k < 4 {
		k = 4
	}
	return k
}

func splitIntoChunks(numbers []int) [][]int {
	n := len(numbers)
	k := numChunksFor(n)

	base := n / k
	rem := n % k

	chunks := make([][]int, 0, k)
	start := 0
	for i := 0; i < k; i++ {
		size := base
		if i < rem {
			size++
		}
		end := start + size
		if end > n {
			end = n
		}
		chunks = append(chunks, numbers[start:end]) // can be empty if k > n (OK)
		start = end
	}
	return chunks
}

// -----------------------------
// Concurrent sorting
// -----------------------------

func sortChunksConcurrently(chunks [][]int) {
	var wg sync.WaitGroup
	wg.Add(len(chunks))

	for i := range chunks {
		i := i
		go func() {
			defer wg.Done()
			sort.Ints(chunks[i]) // sorting empty/1-element chunks is fine
		}()
	}

	wg.Wait()
}

// -----------------------------
// Merge logic (k-way merge via min-heap)
// -----------------------------

type heapItem struct {
	val int
	ci  int // chunk index
	pi  int // position in chunk
}

type minHeap []heapItem

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].val < h[j].val }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)        { *h = append(*h, x.(heapItem)) }
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func mergeSortedChunks(chunks [][]int) []int {
	total := 0
	for _, c := range chunks {
		total += len(c)
	}

	h := &minHeap{}
	heap.Init(h)

	for ci, c := range chunks {
		if len(c) > 0 {
			heap.Push(h, heapItem{val: c[0], ci: ci, pi: 0})
		}
	}

	out := make([]int, 0, total)
	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		out = append(out, item.val)

		next := item.pi + 1
		if next < len(chunks[item.ci]) {
			heap.Push(h, heapItem{val: chunks[item.ci][next], ci: item.ci, pi: next})
		}
	}
	return out
}

// -----------------------------
// Helpers
// -----------------------------

// Random integers range (documented): 0..999 inclusive
func generateRandomNumbers(n int) []int {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = rng.Intn(1000) // 0..999
	}
	return out
}

func printChunks(chunks [][]int) {
	for i, c := range chunks {
		fmt.Printf("Chunk %d: %v\n", i+1, c)
	}
}

func readIntsFromFile(path string) ([]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Error: file not found or cannot open: %v", err)
	}
	defer f.Close()

	var nums []int
	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		v, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("Error: invalid integer on line %d: %q", lineNo, line)
		}
		nums = append(nums, v)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("Error: reading file failed: %v", err)
	}
	return nums, nil
}

func writeIntsToFile(path string, nums []int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, v := range nums {
		if _, err := fmt.Fprintf(w, "%d\n", v); err != nil {
			return err
		}
	}
	return w.Flush()
}
