package writer

import (
	"log"
	"sync"
)

// Multee implements the `SpadeWriter` interface and forwards all calls
// to a slice of targets.
type Multee struct {
	// targets is the spadewriters we will Multee events to
	targets []SpadeWriter
	sync.RWMutex
}

// Add adds a new writer to the slice
func (t *Multee) Add(w SpadeWriter) {
	t.Lock()
	defer t.Unlock()
	t.targets = append(t.targets, w)
}

// AddMany adds muliple writers to the slice
func (t *Multee) AddMany(ws []SpadeWriter) {
	t.Lock()
	defer t.Unlock()
	t.targets = append(t.targets, ws...)
}

// Write forwards a writerequest to multiple targets
func (t *Multee) Write(r *WriteRequest) error {
	t.RLock()
	defer t.RUnlock()

	for i, writer := range t.targets {
		// losing errors here. Alternatives are to
		// not forward events to writers further down the
		// chain, or to return an arbitrary error out of
		// all possible ones that occured
		err := writer.Write(r)
		if err != nil {
			log.Println("Error forwarding event to writer index", i)
		}
	}
	return nil
}

// Rotate forwards a rotation request to multiple targets
func (t *Multee) Rotate() (bool, error) {
	t.RLock()
	defer t.RUnlock()

	allDone := true
	for i, writer := range t.targets {
		// losing errors here. Alternatives are to
		// not rotate writers further down the
		// chain, or to return an arbitrary error
		// out of all possible ones that occured
		done, err := writer.Rotate()
		if err != nil {
			log.Println("Error forwarding rotation request to writer at index", i)
			allDone = false
		} else {
			allDone = allDone && done
		}
	}
	return allDone, nil
}

// Close closes all the target writers, it does this asynchronously
func (t *Multee) Close() error {
	t.Lock()
	defer t.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(t.targets))

	for idx, writer := range t.targets {
		// losing errors here. Alternative is to
		// return an arbitrary error out of all
		// possible ones that occured
		go func(w SpadeWriter, i int) {
			defer wg.Done()
			err := w.Close()
			if err != nil {
				log.Println("Error forwarding rotation request to writer at index", i)
			}
		}(writer, idx)
	}

	wg.Wait()
	return nil
}
