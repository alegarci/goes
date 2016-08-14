package goes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type bulkIndexr struct {
	buf        bytes.Buffer
	bulkChan   chan []byte
	errChan    chan error
	stopChan   chan empty
	routines   routinePool
	timeout    int
	terminated bool
}

// NewBulkIndexr
// maxNumberOfConns is the number of http connections we can make to elasticsearch
// Moreover, the maximum amount of runtime memory needed is maxNumberOfConns * bulkSize
// The latter is because once we have the buffer full, we make a copy and send it. We make the copy to speed up things
func NewBulkIndexr(maxNumberOfConns, bulkSize, timeout int) *bulkIndexr {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if maxNumberOfConns <= 0 {
		maxNumberOfConns = 1
	}
	if maxNumberOfConns >= MaxNumberOfConnection {
		maxNumberOfConns = MaxNumberOfConnection
	}
	return &bulkIndexr{
		buf:        bytes.NewBuffer(make([]byte, bulkSize, 0)),
		bulkChan:   make(chan []byte),
		errChan:    make(chan error),
		stopChan:   make(chan empty),
		routines:   routinePool(maxNumberOfConns),
		timeout:    timeout,
		terminated: false,
	}
}

// Index
func (bi *bulkIndexr) Index(index, typ string, id string, refresh bool, data interface{}) error {
	b, err := writeBulkBytes("index", index, typ, id, data)
	if err != nil {
		return err
	}
	if bi.terminated {
		bi.stopChan <- empty{}
	}
	bi.bulkChan <- b
	return nil
}

func (bi *bulkIndexr) Start() {
	if bi.terminated {
		return
	}
	ticker := time.NewTicker(bi.timeout * time.Millisecond)
	go func() {
		for {
			select {
			case ticker.C:
				bi.flush()
			case b := <-bi.bulkChan:
				if bi.buf.Len()+len(b) > bi.buf.Cap() {
					bi.flush()
				}
				_, err := bi.buf.Write(b)
				if err != nil {
					bi.errChan <- err
				}
			case <-bi.stopChan:
				// signal back to stop func that we are done
				// whatever was in buffer will be lost
				bi.stopChan <- empty{}
				break
			}
		}
	}()
}

// flush
func (bi *bulkIndexr) flush() {
	if bi.buf.Len() == 0 {
		return
	}
	bi.routines.next()
	// create copy of buf
	go func(buf bytes.Buffer) {
		defer bi.routines.add()
		err := send(buf)
		if err != nil {
			bi.errChan <- err
		}

	}(bi.buf)
	bi.buf.Reset()
}

// This does the actual send of a buffer, which has already been formatted
// into bytes of ES formatted bulk data
func send(buf *bytes.Buffer) error {
	type responseStruct struct {
		Took   int64                    `json:"took"`
		Errors bool                     `json:"errors"`
		Items  []map[string]interface{} `json:"items"`
	}

	//response := responseStruct{}

	//body, err := b.conn.DoCommand("POST", fmt.Sprintf("/_bulk?refresh=%t", b.Refresh), nil, buf)
	//
	//if err != nil {
	//	return err
	//}
	//// check for response errors, bulk insert will give 200 OK but then include errors in response
	//jsonErr := json.Unmarshal(body, &response)
	//if jsonErr == nil {
	//	if response.Errors {
	//		// better parse the error message!!, this one sucks!
	//		return fmt.Errorf("Bulk Insertion Error. Failed item count [%d]", len(response.Items))
	//	}
	//}
	return nil
}

// Stop
func (bi *bulkIndexr) Stop() {
	bi.terminated = true
	bi.stopChan <- empty{}
	// we need to wait for the start dne signal
	<-bi.stopChan
	// we wait to see if Index still receiving data even after we call stop
	// we we wait for second signal or until ticker timeout
	ticker := time.NewTicker(500 * time.Millisecond)
	// whatever happens first
	select {
	case <-bi.stopChan:
	case <-ticker.C:
	}

	close(bi.stopChan)
	close(bi.errChan)
	close(bi.bulkChan)
	bi.routines.stop()
}

func (bi *bulkIndexr) ErrCh() chan error {
	if bi.terminated {
		return nil
	}
	return bi.errChan
}

// WriteBulkBytes
// http://www.elasticsearch.org/guide/reference/api/bulk.html
func writeBulkBytes(op string, index, typ, id string, data interface{}) ([]byte, error) {
	// First line
	buf := bytes.NewBuffer([]bytes{})
	buf.WriteString(fmt.Sprintf(`{"%s":{`, op))
	buf.WriteString(fmt.Sprintf(`{"%s":{"_index":"`, op))
	buf.WriteString(index)
	buf.WriteString(`","_type":"`)
	buf.WriteString(typ)
	buf.WriteString(`"`)
	if len(id) > 0 {
		buf.WriteString(`,"_id":"`)
		buf.WriteString(id)
		buf.WriteString(`"`)
	}

	buf.WriteString(`}}`)
	buf.WriteRune('\n')

	// data payload
	switch v := data.(type) {
	case *bytes.Buffer:
		_, err := io.Copy(&buf, v)
		if err != nil {
			return nil, err
		}
	case []byte:
		_, err := buf.Write(v)
		if err != nil {
			return nil, err
		}
	case string:
		_, err := buf.WriteString(v)
		if err != nil {
			return nil, err
		}
	default:
		body, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		_, err := buf.Write(body)
		if err != nil {
			return nil, err
		}
	}
	buf.WriteRune('\n')
	return buf.Bytes(), nil
}
