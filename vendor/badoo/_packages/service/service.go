package service

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"badoo/_packages/service/config"
	"badoo/_packages/service/stats"
	"badoo/_packages/util/osutil"
	"badoo/_packages/util/structs"

	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gogo/protobuf/proto"
)

// Command line flags
type Flags struct {
	ConfFile    string
	LogFile     string
	PidFile     string
	Testconf    bool
	Version     bool
	FullVersion bool
	Debug       bool
}

// Port to listen and serve requests on, stats ports added automatically
// see GpbPort and JsonPort functions below
type Port struct {
	Name      string             // interface name
	Handler   interface{}        // handler context
	Proto     gpbrpc.Protocol    // abstract request -> fanout to request callbacks
	Codec     gpbrpc.ServerCodec // read/write request from/to the network
	IsStarted bool               // has the port been started by now (TODO: allow multiple startups for the same protocol)
}

type Server struct {
	Name    string
	Address string
	Server  *gpbrpc.Server
}

type Config interface {
	GetDaemonConfig() *badoo_config.ServiceConfigDaemonConfigT
}

var (
	flags = Flags{}

	// our private copy of config, in case user doesn't care about the generic part (most of them don't)
	config Config

	commandLine string
	binaryPath  string
	configPath  string

	startupTime       time.Time     // plain startup time (init function)
	initPhaseDuration time.Duration // time from startupTime to entering EventLoop()
	numCPU            int           // number of CPUs we're configured to use

	// started servers: proto -> server
	StartedServers = make(map[string]*Server)

	HttpServer *httpServer

	pidfile *Pidfile

	restartData *RestartChildData

	hostname string // local hostname, fully qualified
)

func init() {
	startupTime = time.Now()
}

func Initialize(default_config_path string, service_conf Config) {
	flag.StringVar(&flags.ConfFile, "c", default_config_path, "path to config file")
	flag.StringVar(&flags.LogFile, "l", "", "path to log file, special value '-' means 'stdout'")
	flag.StringVar(&flags.PidFile, "p", "", "path to pid file. if empty, pidfile will not be created")
	flag.BoolVar(&flags.Testconf, "t", false, "test configuration and exit")
	flag.BoolVar(&flags.Version, "v", false, "print version")
	flag.BoolVar(&flags.FullVersion, "V", false, "print full version info")
	flag.BoolVar(&flags.Debug, "debug", false, "force DEBUG log level")
	flag.Parse()

	if flags.Version {
		fmt.Printf("%s\n", VersionInfo.GetVersion())
		os.Exit(0)
	}

	if flags.FullVersion {
		data, _ := json.MarshalIndent(VersionInfo, "", "  ")
		fmt.Printf("%s\n", data)
		os.Exit(0)
	}

	var err error

	config = service_conf                    // save a pointer to service's config (NOT a copy, mon!)
	commandLine = strings.Join(os.Args, " ") // XXX(antoxa): couldn't think of a better way
	hostname = getHostname()                 // get hostname early

	// moved here from init(), just importing a package should not publish expvars
	initExpvars()

	// current executable full path (symlinks and shit sometimes complicate things)
	binaryPath = func() string {
		path, err := osutil.GetCurrentBinary()
		if err != nil {
			// log as notice, non-critical error (only stats affected)
			log.Infof("couldn't get current binary (using argv[0] = %q): %v", os.Args[0], err)
			return os.Args[0]
		}
		return path
	}()

	// config path
	confPath := func() string {
		if flags.ConfFile != "" {
			return flags.ConfFile
		}
		return default_config_path
	}()

	// resolve absolute config path, convenient for stats
	configPath = func(path string) string {
		var err error
		if path, err = filepath.Abs(path); err != nil {
			return path
		}
		if path, err = filepath.EvalSymlinks(path); err != nil {
			return path
		}
		return path
	}(confPath)

	// parse config and construct final config merged with command line flags
	// use path as supplied to us in args (i.e. unresolved), just to avoid 'too smart, outsmarted yourself' gotchas
	err = ParseConfigFromFile(confPath, service_conf)
	if err != nil {
		err_message := func(err error) string {
			switch real_err := err.(type) {
			case nil:
				return "syntax is ok"
			case *json.SyntaxError:
				return fmt.Sprintf("%v at offset %d", real_err, real_err.Offset)
			case *os.PathError:
				return fmt.Sprintf("%v", real_err)
			default:
				return fmt.Sprintf("(%T) %v", real_err, real_err)
			}
		}(err)

		stderrLogger.Fatalf("Error in config: %s", err_message)

	} else {
		if flags.Testconf {
			fmt.Printf("testconf %s: syntax is ok\n", configPath)
		}
	}

	mergeCommandlineFlagsToConfig(flags, config)

	daemonConfig := config.GetDaemonConfig()

	// antoxa: need the fancy wrapper function to have testconf behave properly
	// FIXME: testconf should check more stuff (below) and graceful restart also
	initPidfileLogfile := func() error {

		// FIXME(antoxa): this testconf thingy is everywhere! must... resist... full rewrite
		if flags.Testconf {
			err = PidfileTest(daemonConfig.GetPidFile())
		} else {
			pidfile, err = PidfileOpen(daemonConfig.GetPidFile())
		}

		if err != nil {
			return fmt.Errorf("can't open pidfile: %s", err)
		}

		// FIXME: this is shit ugly
		//  need better integration between logger and daemon-config
		//  or 1-to-1 mapping
		//  or better log package :)
		log_level := daemonConfig.GetLogLevel()
		if log_level == 0 {
			return fmt.Errorf("unknown log_level, supported: %v", badoo_config.ServiceConfigDaemonConfigTLogLevels_name)
		}
		err = reopenLogfile(daemonConfig.GetLogFile(), log.Level(log_level))
		if err != nil {
			return fmt.Errorf("can't open logfile: %s", err)
		}

		return nil
	}

	err = initPidfileLogfile()
	if err != nil {
		if flags.Testconf {
			stderrLogger.Errorf("%v", err)
			fmt.Printf("testconf failed\n")
		} else {
			stderrLogger.Errorf("%v", err) // always pidfile/logfile errors to stderr
		}
		os.Exit(1)
	} else {
		if flags.Testconf {
			fmt.Printf("testconf successful\n")
			os.Exit(0)
		}
	}

	// log some version info like libangel does
	versionString := func() string {
		vi := &VersionInfo
		version := func() string {
			if vi.GetAutoBuildTag() != "" {
				return fmt.Sprintf("%s-%s", vi.GetVersion(), vi.GetAutoBuildTag())
			} else {
				return vi.GetVersion()
			}
		}()
		return fmt.Sprintf("%s version %s, git %s, built %s on %s",
			vi.GetVcsBasename(), version, vi.GetVcsShortHash(), vi.GetBuildDate(), vi.GetBuildHost())
	}()
	log.Infof("%s", versionString)

	// max cpus, 0 = all of them
	numCPU := func() int {
		maxCpus := int(daemonConfig.GetMaxCpus())
		if maxCpus <= 0 || maxCpus > runtime.NumCPU() {
			maxCpus = runtime.NumCPU()
		}
		return maxCpus
	}()
	runtime.GOMAXPROCS(numCPU)

	// gc percent, <0 - disables GC
	if daemonConfig.GcPercent != nil {
		debug.SetGCPercent(int(daemonConfig.GetGcPercent()))
	}

	// process pinba configuration and related stuff
	pinbaSender, err = func() (*PinbaSender, error) { // assigns a global
		if daemonConfig.GetPinbaAddress() == "" {
			return nil, nil // user doesn't want pinba configured
		}

		pi, err := PinbaInfoFromConfig(config)
		if err != nil {
			return nil, err
		}

		return NewPinbaSender(pi), nil
	}()

	if err != nil {
		log.Fatalf("pinba config error: %v", err)
	}

	// graceful restart handling
	//  see restart.go and signals.go for more details
	restartData, err = ParseRestartDataFromEnv()
	if err != nil {
		log.Fatalf("can't parse restart data: %v", err)
	}
	if restartData != nil {
		log.Debugf("[CHILD] this is a restart, parent: %d, me: %d", restartData.PPid, os.Getpid())
	}

	// start http pprof server (possibly - inherit fd from parent if this is a restart)
	err = func() (err error) {
		HttpServer, err = newHttpServer(config, restartData) // assigning a global here
		if err != nil {
			return err
		}

		if HttpServer != nil { // nil here means it has not been configured
			go HttpServer.Serve()
		}

		return nil
	}()
	if err != nil {
		log.Fatalf("can't start http_pprof server: %v", err)
	}
}

// Call this when you want to start your servers and stuff
func EventLoop(ports []Port) {
	defer log.Debug("exiting")

	initPhaseDuration = time.Since(startupTime)

	daemonConfig := config.GetDaemonConfig()

	// service-stats ports
	ports = append(ports, GpbPort("service-stats-gpb", stats_ctx, badoo_service.Gpbrpc))
	ports = append(ports, JsonPort("service-stats-gpb/json", stats_ctx, badoo_service.Gpbrpc))

	// build map of ports and do some sanity checks
	ph := make(map[string]*Port)
	for i := 0; i < len(ports); i++ {
		p := &ports[i]
		ph[p.Name] = p

		// json and gpb ports should have the same context
		//  so try and warn user about passing plain values in (as it makes a copy)
		if reflect.ValueOf(p.Handler).Kind() != reflect.Ptr {
			log.Infof("port[%d].Handler should be a pointer (you want gpbs and json to use the same context, right?) (now: %T)", i, p.Handler)
		}
	}

	getRestartSocket := func(rcd *RestartChildData, portName, portAddr string) (*RestartSocket, *os.File) {
		if rcd == nil {
			return nil, nil
		}

		restartSocket, exists := rcd.GpbrpcSockets[portName]
		if exists == false {
			return nil, nil
		}

		restartFile := os.NewFile(restartSocket.Fd, "")

		if restartSocket.Address != portAddr {
			return nil, restartFile
		}

		return &restartSocket, restartFile
	}

	// start 'em all
	for _, lcf := range daemonConfig.GetListen() {
		portName, portAddr := lcf.GetProto(), lcf.GetAddress()
		port := ph[portName]

		if nil == port {
			log.Warnf("ignoring unknown port: %s at %s", portName, portAddr)
			continue
		}

		if port.IsStarted {
			log.Warnf("ignoring double startup for port: %s at %s", portName, portAddr)
		}

		listener, err := func() (listener net.Listener, err error) { // it's important that this should be a function, see defer inside
			restartSocket, restartFile := getRestartSocket(restartData, portName, portAddr)

			// this whole fd/file affair is very inconvenient to
			//  since when getRestartSocket() returns fd - it can't close it yet, as it can be used by FileListener
			defer restartFile.Close()

			if restartSocket == nil {
				listener, err = net.Listen("tcp", portAddr)
				if err != nil {
					log.Errorf("listen failed for server %s at %s: %s", portName, portAddr, err)
					return
				}
				log.Infof("port %s bound to address %s", portName, listener.Addr())

			} else {

				listener, err = net.FileListener(restartFile) // this dup()-s
				if err != nil {
					log.Errorf("failed to grab parent fd %d for %s at %s: %s", restartSocket.Fd, portName, portAddr, err)
					return
				}

				log.Infof("port %s bound to address %s (parent fd: %d)", portName, listener.Addr(), restartSocket.Fd)
			}
			return
		}()

		if err != nil {
			os.Exit(1)
		}

		// enable pinba only for ports that explicitly request it
		ps := func() gpbrpc.PinbaSender {
			if !lcf.GetPinbaEnabled() {
				return nil // explicit nil here
			}

			if pinbaSender == nil {
				log.Warnf("pinba is not configured, but pinba_enabled IS set for port %s: %s", portName, portAddr)
				return nil // explicit nil here
			}

			log.Infof("pinba configured for port %s:%s -> %s", portName, portAddr, pinbaSender.Address)
			return pinbaSender
		}()

		// slow request log time
		slowRequestTime := time.Duration(daemonConfig.GetSlowRequestMs()) * time.Millisecond

		srv := &Server{
			Name:    lcf.GetProto(),
			Address: lcf.GetAddress(),
			Server:  gpbrpc.NewServer(listener, port.Proto, port.Codec, port.Handler, ps, slowRequestTime),
		}
		go srv.Server.Serve()

		port.IsStarted = true
		StartedServers[port.Name] = srv // save it for laterz
	}

	// kill parent if this is a child of graceful restart
	if restartData != nil {
		syscall.Kill(restartData.PPid, syscall.SIGQUIT)
	}

	log.Infof("entering event loop")

	exitMethod := wait_for_signals()

	if exitMethod == EXIT_GRACEFULLY { // wait for established connections to close and then die

		// FIXME: should stop servers from accepting new connections here!

		const ATTEMPTS_PER_SEC = 2
		maxAttempts := daemonConfig.GetParentWaitTimeout() * ATTEMPTS_PER_SEC

		for i := uint32(0); i < maxAttempts; i++ {
			for _, srv := range StartedServers {
				currConn := atomic.LoadUint64(&srv.Server.Stats.ConnCur)
				if currConn > 0 {
					log.Debugf("%s still has %d connections", srv.Name, currConn)
					time.Sleep(time.Second / ATTEMPTS_PER_SEC)
				}
			}
		}
	} else {
		// do nothing for EXIT_IMMEDIATELY
	}

	// doing cleanups here
	// XXX: can this be moved to defer at the start of this function?
	if pidfile != nil {
		pidfile.CloseAndRemove()
	}
}

func CmdlineFlags() *Flags {
	return &flags
}

func Hostname() string {
	return hostname
}

func DaemonConfig() *badoo_config.ServiceConfigDaemonConfigT {
	return config.GetDaemonConfig()
}

func GpbPort(name string, handler interface{}, rpc gpbrpc.Protocol) Port {
	return Port{name, handler, rpc, &gpbrpc.GpbsCodec, false}
}

func JsonPort(name string, handler interface{}, rpc gpbrpc.Protocol) Port {
	return Port{name, handler, rpc, &gpbrpc.JsonCodec, false}
}

func GetNumCPU() int {
	return numCPU
}

func GetHostname() string {
	return hostname
}

func GetInitPhaseDuration() time.Duration {
	return initPhaseDuration
}

func GetStartupTime() time.Time {
	return startupTime
}

// ----------------------------------------------------------------------------------------------------------------------------------

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Warnf("can't get local hostname: %v", err)
		return "-"
	}

	cname, err := net.LookupCNAME(hostname)
	if err != nil {
		log.Warnf("can't get FQDN for %s (using as is): %v", hostname, err)
		return hostname
	}

	if cname[len(cname)-1] == '.' {
		cname = cname[:len(cname)-1]
	}

	return cname
}

func mergeCommandlineFlagsToConfig(flags Flags, config Config) {
	// merge the rest of command line arguments into config
	if "" != flags.LogFile {
		config.GetDaemonConfig().LogFile = proto.String(flags.LogFile)
	}

	if "" != flags.PidFile {
		config.GetDaemonConfig().PidFile = proto.String(flags.PidFile)
	}

	if flags.Debug {
		config.GetDaemonConfig().LogLevel = badoo_config.ServiceConfigDaemonConfigT_DEBUG.Enum()
	}
}

func initExpvars() {
	expvar.Publish("__binary_path", expvar.Func(func() interface{} { return &binaryPath }))
	expvar.Publish("__config_path", expvar.Func(func() interface{} { return &configPath }))
	expvar.Publish("_command_line", expvar.Func(func() interface{} { return &commandLine }))
	expvar.Publish("_version", expvar.Func(func() interface{} { return VersionInfo }))
	expvar.Publish("_hostname", expvar.Func(func() interface{} { return hostname }))

	expvar.Publish("_config", expvar.Func(func() interface{} {
		cf, err := structs.BadooStripSecretFields(config) // makes a copy of `config`
		if err != nil {
			return struct {
				Error string `json:"error"`
			}{err.Error()}
		}
		return cf
	}))

	expvar.Publish("_service-stats", expvar.Func(func() interface{} {
		stats, err := GatherServiceStats()
		if err != nil {
			return struct {
				Error string `json:"error"`
			}{err.Error()}
		}
		return stats
	}))
}
