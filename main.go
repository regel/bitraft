package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hashicorp/go-sockaddr/template"
	"github.com/prologic/bitcask"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"github.com/tidwall/redcon"
	"github.com/tidwall/sds"
	"github.com/tidwall/uhaha"
)

var (
	debug           bool
	version         bool
	maxKeySize      int
	maxValueSize    int
	maxDatafileSize int

	bind string
	path string

	logdir string

	join      string
	advertise string

	conf uhaha.Config
	db   *bitcask.Bitcask
)

func init() {
	conf.Flag.Custom = true
	conf.Name = "bitraft"
	conf.Version = FullVersion()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.BoolVarP(&version, "version", "v", false, "Display version information")
	flag.BoolVarP(&debug, "debug", "d", false, "Enable debug logging")

	flag.IntVarP(&maxKeySize, "max-key-size", "K", 64, "Set maximum key size in bytes")
	flag.IntVarP(&maxValueSize, "max-value-size", "V", 65535, "Set maximum value size in bytes")
	flag.IntVarP(&maxDatafileSize, "max-datafile-size", "M", 1<<20, "Set maximum datafile size in bytes")

	flag.StringVarP(&bind, "bind", "b", ":4920", "Bind interface to listen to")
	flag.StringVarP(&path, "path", "p", "data", "Path to data directory")

	flag.StringVarP(&logdir, "logdir", "l", "", "Set log directory. If blank it will equals --data")

	flag.StringVarP(&join, "join", "j", "", "Join a cluster by providing an address")
	flag.StringVarP(&advertise, "advertise", "a", ":5920", "Advertise interface")
}

func mustParseAddr(addr string) string {
	r, err := template.Parse(addr)
	if err != nil {
		log.WithError(err).Fatalf("error parsing addr %s: %s", addr, err)
	}
	return r
}

func newDatabase(path string) *bitcask.Bitcask {
	if err := os.RemoveAll(path); err != nil {
		// XXX: Would really prefer to return an error here rather than panic()
		log.WithError(err).Fatal("error removing path %s", path)
	}

	// TODO: How do I pass WithXXX() options?
	db, err := bitcask.Open(path)
	if err != nil {
		// XXX: Would really prefer to return an error here rather than panic()
		log.WithError(err).Fatalf("error opening database")
	}
	return db
}

func tick(m uhaha.Machine) {}

// SET key value [EX seconds]
func cmdSET(m uhaha.Machine, args []string) (interface{}, error) {
	db := m.Data().(*bitcask.Bitcask)
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	val := []byte(args[2])

	if err := db.Put(key, val); err != nil {
		return nil, fmt.Errorf("error writing key: %w", err)
	}

	return redcon.SimpleString("OK"), nil
}

// DBSIZE
func cmdDBSIZE(m uhaha.Machine, args []string) (interface{}, error) {
	db := m.Data().(*bitcask.Bitcask)
	if len(args) != 1 {
		return nil, uhaha.ErrWrongNumArgs
	}
	return redcon.SimpleInt(db.Len()), nil
}

// GET key
func cmdGET(m uhaha.Machine, args []string) (interface{}, error) {
	db := m.Data().(*bitcask.Bitcask)
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])
	val, err := db.Get(key)
	if err != nil {
		if err == bitcask.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("error reading key: %s", err)
	}

	return val, nil
}

// KEYS pattern
// help: return a list of all keys matching the provided pattern
func cmdKEYS(m uhaha.Machine, args []string) (interface{}, error) {
	db := m.Data().(*bitcask.Bitcask)
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	prefix := []byte(args[1])

	var keys []string

	err := db.Scan(prefix, func(key []byte) error {
		keys = append(keys, string(key))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error scanning keys: %w", err)
	}

	return keys, nil
}

// DEL key [key ...]
// help: delete one or more keys. Returns the number of keys deleted.
func cmdDEL(m uhaha.Machine, args []string) (interface{}, error) {
	db := m.Data().(*bitcask.Bitcask)
	if len(args) < 2 {
		return nil, uhaha.ErrWrongNumArgs
	}

	key := []byte(args[1])

	if err := db.Delete(key); err != nil {
		return nil, fmt.Errorf("error deleting key: %w", err)
	}

	return redcon.SimpleInt(1), nil
}

// MONITOR
// help: monitors all of the commands from all clients
func cmdMONITOR(m uhaha.Machine, args []string) (interface{}, error) {
	// Here we'll return a Hijack type that is just a function that will
	// take over the client connection in an isolated context.
	return uhaha.Hijack(hijackedMONITOR), nil
}

func hijackedMONITOR(s uhaha.Service, conn uhaha.HijackedConn) {
	obs := s.Monitor().NewObserver()
	s.Log().Printf("hijack opened: %s", conn.RemoteAddr())
	defer func() {
		s.Log().Printf("hijack closed: %s", conn.RemoteAddr())
		obs.Stop()
		conn.Close()
	}()
	conn.WriteAny(redcon.SimpleString("OK"))
	conn.Flush()
	go func() {
		defer obs.Stop()
		for {
			// Wait for any incoming command or error and immediately kill
			// the connection, which will in turn stop the observer.
			if _, err := conn.ReadCommand(); err != nil {
				return
			}
		}
	}()
	// Range over the observer's messages and send to the hijacked client.
	for msg := range obs.C() {
		var args string
		for i := 0; i < len(msg.Args); i++ {
			args += " " + strconv.Quote(msg.Args[i])
		}
		conn.WriteAny(redcon.SimpleString(fmt.Sprintf("%0.6f [0 %s]%s",
			float64(time.Now().UnixNano())/1e9, msg.Addr, args,
		)))
		conn.Flush()
	}
}

// #region -- SNAPSHOT & RESTORE

type dbSnapshot struct {
	db *bitcask.Bitcask
}

func (s *dbSnapshot) Persist(wr io.Writer) error {
	w := sds.NewWriter(wr)

	err := s.db.Fold(func(key []byte) error {
		val, err := db.Get(key)
		if err != nil {
			return err
		}

		if err := w.WriteUint64(uint64(len(key))); err != nil {
			return err
		}
		if err := w.WriteBytes(key); err != nil {
			return err
		}

		if err := w.WriteUint64(uint64(len(val))); err != nil {
			return err
		}
		if err := w.WriteBytes(val); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return w.Flush()
}

func (s *dbSnapshot) Done(path string) {
	if path != "" {
		// snapshot was a success.
	}
}

func snapshot(data interface{}) (uhaha.Snapshot, error) {
	db := data.(*bitcask.Bitcask)
	return &dbSnapshot{db}, nil
}

func restore(rd io.Reader) (interface{}, error) {
	// TODO: I don't know how to implement this!
	// XXX: Where do I get the original Datadir from?
	return nil, fmt.Errorf("error restore not implemented")
}

func main() {
	flag.Parse()

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if version {
		fmt.Printf("bitraft version %s", FullVersion())
		os.Exit(0)
	}

	conf.DataDir = path
	conf.Addr = mustParseAddr(bind)
	conf.JoinAddr = mustParseAddr(join)
	conf.Advertise = mustParseAddr(advertise)
	conf.LogOutput = log.StandardLogger().Writer()
	conf.InitialData = newDatabase(filepath.Join(path, "db"))
	conf.Snapshot = snapshot
	conf.Restore = restore
	conf.Tick = tick

	conf.AddWriteCommand("set", cmdSET)
	conf.AddWriteCommand("del", cmdDEL)
	conf.AddReadCommand("get", cmdGET)
	conf.AddReadCommand("keys", cmdKEYS)
	conf.AddReadCommand("dbsize", cmdDBSIZE)
	conf.AddIntermediateCommand("monitor", cmdMONITOR)

	uhaha.Main(conf)
}
