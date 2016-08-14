package goes

const (
	// two seconds
	DefaultTimeout = 2000
	// by elastic search documentation
	MaxNumberOfConnection = 50
)

type Indexr interface {
	Index(index, _type, id string, refresh bool, data interface{}) error
	Start()
	Stop()
	ErrCh() chan error
}

type empty struct{}

type routinePool struct {
	pool chan empty
}

func routinePool(maxNumberOfRoutines int) routinePool {
	return routinePool{
		pool: make(chan empty,maxNumberOfRoutines),
	}
}

func (rp routinePool) next() {
	<- rp.pool
}

func (rp routinePool) add() {
	rp.pool<-empty{}
}

func (rp routinePool) stop() {
	close(rp.pool)
}
