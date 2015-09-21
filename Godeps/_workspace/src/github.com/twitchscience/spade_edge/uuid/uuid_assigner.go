package uuid

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"strconv"
	"time"
)

type UUIDAssigner interface {
	Assign() string
}

type SpadeUUIDAssigner struct {
	host         string
	cluster      string
	secondTicker <-chan time.Time
	assign       chan string
	count        uint64
	fixedString  string
}

func StartUUIDAssigner(host string, cluster string) UUIDAssigner {
	h := md5.New()
	h.Write([]byte(host))
	host = fmt.Sprintf("%08x", h.Sum(nil)[:4])

	h = md5.New()
	h.Write([]byte(cluster))
	cluster = fmt.Sprintf("%08x", h.Sum(nil)[:4])

	a := &SpadeUUIDAssigner{
		host:         host,
		cluster:      cluster,
		secondTicker: time.Tick(1 * time.Second),
		assign:       make(chan string),
		count:        0,
		fixedString:  fmt.Sprintf("%s-%s-", host, cluster),
	}
	go a.crank()
	return a
}

func (a *SpadeUUIDAssigner) makeId(currentTimeHex, countHex string, buf *bytes.Buffer) {
	buf.Reset()
	buf.WriteString(a.fixedString)
	buf.WriteString(currentTimeHex)
	buf.WriteString("-")
	buf.WriteString(countHex)
}

func (a *SpadeUUIDAssigner) crank() {
	currentTimeHex := strconv.FormatInt(time.Now().Unix(), 16)
	countHex := strconv.FormatUint(a.count, 16)
	buf := bytes.NewBuffer(make([]byte, 0, 34))
	a.makeId(currentTimeHex, countHex, buf)
	for {
		select {
		case <-a.secondTicker:
			currentTimeHex = strconv.FormatInt(time.Now().Unix(), 16)
			a.count = 0
		case a.assign <- buf.String():
			a.count++
			countHex := strconv.FormatUint(a.count, 16)
			a.makeId(currentTimeHex, countHex, buf)
		}
	}
}

func (a *SpadeUUIDAssigner) Assign() string {
	return <-a.assign
}
