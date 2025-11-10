package simplejsondb

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
)

// helper: returns the RWMutex for a specific record ID, creating it if needed
func (c *collection) newLock(id string) *sync.RWMutex {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recLocks == nil {
		c.recLocks = make(map[string]*sync.RWMutex)
	}
	l, ok := c.recLocks[id]
	if !ok || l == nil {
		l = &sync.RWMutex{}
		c.recLocks[id] = l
	}
	return l
}

// helper: returns the WaitGroup for a specific record ID, creating it if needed (reuses existing)
func (c *collection) newWg(id string) *sync.WaitGroup {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recWg == nil {
		c.recWg = make(map[string]*sync.WaitGroup)
	}
	if wg, ok := c.recWg[id]; ok && wg != nil {
		return wg
	}
	wg := &sync.WaitGroup{}
	c.recWg[id] = wg
	return wg
}

// helper: returns the RWMutex for a specific record ID if it exists; does not create it
func (c *collection) getLockIfExists(id string) *sync.RWMutex {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recLocks == nil {
		return nil
	}
	return c.recLocks[id]
}

// helper: returns the WaitGroup for a specific record ID if it exists; does not create it
func (c *collection) getWGIfExists(id string) *sync.WaitGroup {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recWg == nil {
		return nil
	}
	return c.recWg[id]
}

// helper: returns the LockState for a specific ID, creating it if needed
func (c *collection) doState(id string) *LockState {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recStates == nil {
		c.recStates = make(map[string]*LockState)
	}
	st, ok := c.recStates[id]
	if !ok || st == nil {
		st = &LockState{}
		c.recStates[id] = st
	}
	return st
}

// helper: returns the LockState for a specific ID if it exists; does not create it
func (c *collection) getStateIfExists(id string) *LockState {
	c.recMu.Lock()
	defer c.recMu.Unlock()
	if c.recStates == nil {
		return nil
	}
	return c.recStates[id]
}

// LockID allows manual locking for a specific record ID.
func (c *collection) LockID(id string, mode LockMode) (LockMode, error) {
	if mode == NoMode {
		return NoMode, nil
	}

	var err error = nil
	// detect possible deadlock due to double locking
	defer func() {
		if r := recover(); r != nil {
			log.Printf("deadlock detected: possible double lock on ID '" + id + "' with mode " + strconv.Itoa(int(mode)))
			err = errors.New(fmt.Sprintf("%v", r))
		}
	}()
	l := c.newLock(id)
	wg := c.newWg(id)
	st := c.doState(id)

	switch mode {
	case ModeRead:
		l.RLock()
		st.R++
		wg.Add(1)
	case ModeReadWrite:
		l.Lock()
		st.W++
		st.R++
		wg.Add(1)
	default: // ModeWrite and any other treated as exclusive
		l.Lock()
		st.W++
		wg.Add(1)
	}
	// update recorded mode to reflect the current state safely
	c.recMu.Lock()
	if c.recModes == nil {
		c.recModes = make(map[string]LockMode)
	}
	if st.W > 0 {
		if st.R > 0 {
			c.recModes[id] = ModeReadWrite
		} else {
			c.recModes[id] = ModeWrite
		}
	} else if st.R > 0 {
		c.recModes[id] = ModeRead
	} else {
		c.recModes[id] = NoMode
	}
	c.recMu.Unlock()

	return mode, err
}

// UnlockID releases a previously acquired lock for a specific record ID.
// mode should match the mode used in LockID.
func (c *collection) UnlockID(id string) error {
	// read current mode under lock for consistency
	c.recMu.Lock()
	mode := c.recModes[id]
	c.recMu.Unlock()
	if mode == NoMode {
		return nil
	}
	var err error = nil
	defer func() {
		if r := recover(); r != nil {
			log.Printf("deadlock detected: possible double unlock on ID '" + id + "' with mode " + strconv.Itoa(int(mode)))
			err = errors.New(fmt.Sprintf("%v", r))
		}
	}()
	// Do not create a new lock when unlocking; if lock/state don't exist, it's a no-op
	l := c.getLockIfExists(id)
	st := c.getStateIfExists(id)
	if l == nil || st == nil {
		return nil
	}

	switch mode {
	case ModeRead:
		if st.R <= 0 {
			err = errors.New("double unlock: read lock not held")
		} else {
			st.R--
			l.RUnlock()
		}
	case ModeReadWrite:
		if st.R <= 0 || st.W <= 0 {
			err = errors.New("double unlock: read/write lock not held")
		} else {
			st.R--
			st.W--
			l.Unlock()
		}
	default: // write/read_write/other (exclusive)
		if st.W <= 0 {
			err = errors.New("double unlock: write lock not held")
		} else {
			st.W--
			l.Unlock()
		}
	}

	if err == nil {
		// decrement waitgroup once per successful unlock
		if wg := c.getWGIfExists(id); wg != nil {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("WaitGroup panic on UnlockID for ID '" + id + "': " + fmt.Sprintf("%v", r))
				}
			}()
			wg.Done()
		}
		// recompute and possibly cleanup maps only when fully unlocked
		c.recMu.Lock()
		if st.W > 0 {
			if st.R > 0 {
				c.recModes[id] = ModeReadWrite
			} else {
				c.recModes[id] = ModeWrite
			}
		} else if st.R > 0 {
			c.recModes[id] = ModeRead
		} else {
			c.recModes[id] = NoMode
			// fully unlocked: cleanup holders
			if c.recLocks != nil {
				delete(c.recLocks, id)
			}
			if c.recStates != nil {
				delete(c.recStates, id)
			}
			if c.recWg != nil {
				delete(c.recWg, id)
			}
		}
		c.recMu.Unlock()
	}

	return err
}

// GetLock returns the RecordLock (RWMutex + LockState) for a specific record ID.
func (c *collection) GetLock(id string) *RecordLock {
	lock := c.getLockIfExists(id)
	if lock == nil {
		return nil
	}
	exists := c.getStateIfExists(id)
	mode := c.recModes[id]
	if exists == nil {
		exists = &LockState{}
	}
	return &RecordLock{
		ID:    id,
		Lock:  lock,
		State: exists,
		Mode:  &mode,
		WG:    c.getWGIfExists(id),
	}
}

func (c *collection) IsLock(id string) bool {
	l := c.getLockIfExists(id)
	st := c.getStateIfExists(id)
	mode := c.recModes[id]
	if l == nil || st == nil {
		return false
	}
	switch mode {
	case ModeRead:
		return st.R > 0
	case ModeReadWrite:
		return st.R > 0 && st.W > 0
	default: // write/read_write/other (exclusive)
		return st.W > 0
	}
}

// WaitUnlock waits until the lock for a specific record ID is released if it exists.
func (c *collection) WaitUnlock(id string) *RecordLock {
	lock := c.GetLock(id)
	if lock == nil {
		return nil
	}
	wg := lock.WG
	if wg != nil {
		wg.Wait()
	}

	return lock
}
