package threader

import (
	"sync"
	"time"
)

func main() {
	wg := sync.WaitGroup{}
	for i,_ := range [10]int{} {
		wg.Add(1)
		go Worker(i, &wg)
	}

	wg.Wait()
}

func Worker(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i :=0; i< 3; i++ {
		message := time.Now().Local().String()
		Dispatch(id, message)
	}
}

func Dispatch(id int, message string) {
	
}