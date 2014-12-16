package server_log

import "bytes"

const (
	IP_GRAB = iota
	TIME_WAIT
	TIME_GRAB
	D
	DA
	DAT
	DATA
	DATA_EQ
	DATA_GRAB
	UUID_WAIT
	UUID_GRAB
	DONE
)

// This is a super ugly state machine to parse Nginx logs and extract info in a single pass
func LexLine(line []byte) *parseResult {
	state := IP_GRAB
	ip := bytes.NewBuffer(make([]byte, 0, 16))
	time := bytes.NewBuffer(make([]byte, 0, 16))
	data := bytes.NewBuffer(make([]byte, 0, 2048))
	uuid := bytes.NewBuffer(make([]byte, 0, 64))
	for _, c := range line {
		switch {
		case state == IP_GRAB && (c == ' ' || c == '|' || c == ','):
			state = TIME_WAIT
		case state == IP_GRAB && c == '-':
			continue
		case state == IP_GRAB && c == '/':
			if ip.Len() > 0 {
				// if we havent grabbed anything yet this means that we dont
				// have a ip forwarded form the load balancer so pick up the string after the slash
				state = TIME_WAIT
			} else {
				continue
			}
		case state == IP_GRAB:
			ip.WriteByte(c)
		case state == TIME_WAIT && c != '[':
			continue
		case state == TIME_WAIT && c == '[':
			state = TIME_GRAB
		case state == TIME_GRAB && c == '.':
			state = D
		case state == TIME_GRAB:
			time.WriteByte(c)
		case state == D && c == 'd':
			state = DA
		case state == D:
			continue
		case state == DA && c == 'a':
			state = DAT
		case state == DA:
			state = D
		case state == DAT && c == 't':
			state = DATA
		case state == DAT:
			state = D
		case state == DATA && c == 'a':
			state = DATA_EQ
		case state == DATA:
			state = D
		case state == DATA_EQ && c == '=':
			state = DATA_GRAB
		case state == DATA_EQ:
			state = D
		case state == DATA_GRAB && (c == '"' || c == '&' || c == ';'):
			state = UUID_WAIT
		case state == DATA_GRAB && c == ' ':
			state = UUID_GRAB
		case state == DATA_GRAB:
			data.WriteByte(c)
		case state == UUID_WAIT && c != ' ':
			continue
		case state == UUID_WAIT && c == ' ':
			state = UUID_GRAB
		case state == UUID_GRAB && c != ' ':
			uuid.WriteByte(c)
			state = UUID_GRAB
		case state == UUID_GRAB && c == ' ':
			state = DONE
		case state == UUID_GRAB && c != ' ':
			uuid.WriteByte(c)
		case state == DONE:
			break
		}
	}
	return &parseResult{
		ip:   ip.String(),
		when: time.String(),
		data: data.Bytes(),
		uuid: uuid.String(),
	}
}
