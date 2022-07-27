package util

import (
	"fmt"
	"sync"
)

type Pool struct {
	mutex sync.Mutex
	count int

	workerChan chan struct{}
	resultChan chan error
}

func NewPool(concurrentCount int) *Pool {
	newPool := &Pool{
		workerChan: make(chan struct{}, concurrentCount),
		resultChan: make(chan error, concurrentCount),
	}
	return newPool
}

func (p *Pool) Run(do func() error) {
	p.mutex.Lock()
	p.count++
	p.mutex.Unlock()

	go func() {
		p.workerChan <- struct{}{}
		err := do()
		<-p.workerChan
		p.resultChan <- err
	}()
}

func (p *Pool) Wait() (errResults []error) {
	var results []error

	for x := range p.resultChan {
		p.mutex.Lock()
		c := p.count
		p.mutex.Unlock()

		if x != nil {
			errResults = append(errResults, x)
		}
		results = append(results, x)
		if len(results) == c {
			close(p.resultChan)
			break
		}
	}

	return
}

func MarshalErrors(errors []error) error {
	var errString string
	for _, x := range errors {
		if x != nil {
			errString += x.Error()
		}
	}

	if len(errString) > 0 {
		return fmt.Errorf(errString)
	}
	return nil
}
