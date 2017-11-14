package service

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"badoo/_packages/service/config"
	"badoo/_packages/service/stats"
	"badoo/_packages/util/debugcharts" // see comments inside, on why we've forked
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
	"syscall"
	"time"

	"github.com/gogo/protobuf/proto"
)

// Command line flags
type flagsCollection struct {
	ConfFile     string
	LogFile      string
	PidFile      string
	Mothership   string
	InstanceName string
	Tag          string
	Testconf     bool
	Version      bool
	FullVersion  bool
	Debug        bool
}

// Port to listen and serve requests on, stats ports added automatically
// see GpbPort and JsonPort functions below
type Port struct {
	Name                string             // interface name
	ConfAddress         string             // address, as given in config
	Handler             interface{}        // handler context
	Proto               gpbrpc.Protocol    // abstract request -> fanout to request callbacks
	IsStarted           bool               // has the port been started by now (TODO: allow multiple startups for the same protocol)
	SlowRequestTime     time.Duration      // log requests that take longer than this amount of time
	MaxParallelRequests uint               // how many parallel requests to accept (works in ports that have multiplexing only (i.e. zmq))
	PinbaSender         gpbrpc.PinbaSender // use this object to send to pinba (can be nil)

	Listen    func(net, laddr string) (net.Listener, error)    // create listener at address
	NewServer func(net.Listener, *Port) (gpbrpc.Server, error) // create server with listener
}

type portServer struct {
	name        string
	confAddress string
	realAddress string
	port        *Port
	server      gpbrpc.Server
}

type Config interface {
	GetDaemonConfig() *badoo_config.ServiceConfigDaemonConfigT
}

var (
	flags = flagsCollection{}

	// full list of ports, user-defined and built-in
	ports []Port

	// our private copy of config, in case user doesn't care about the generic part (most of them don't)
	config Config

	commandLine string
	binaryPath  string
	configPath  string

	startupTime       time.Time     // plain startup time (init function)
	initPhaseDuration time.Duration // time from startupTime to entering EventLoop()
	numCPU            int           // number of CPUs we're configured to use

	// started servers: proto -> portServer
	startedServers = make(map[string]*portServer)

	httpServer *httpServerT

	pidfile *PidfileCtx

	restartData *restartChildData

	hostname string // local hostname, fully qualified

	enableZmqPorts bool
)

func init() {
	startupTime = time.Now()
}

func Initialize(defaultConfigPath string, serviceConf Config) {
	flag.StringVar(&flags.ConfFile, "c", defaultConfigPath, "path to config file")
	flag.StringVar(&flags.ConfFile, "config", defaultConfigPath, "path to config file")
	flag.StringVar(&flags.LogFile, "l", "", "path to log file, special value '-' means 'stdout'")
	flag.StringVar(&flags.PidFile, "p", "", "path to pid file. if empty, pidfile will not be created")
	flag.StringVar(&flags.Mothership, "mothership", "", "connect to mothership at this address")
	flag.StringVar(&flags.InstanceName, "service_instance_name", "", "set instance name to this")
	flag.StringVar(&flags.Tag, "tag", "", "set service tag")
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

	config = serviceConf                     // save a pointer to service's config (NOT a copy, mon!)
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
		return defaultConfigPath
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
	err = ParseConfigFromFile(confPath, serviceConf)
	if err != nil {
		errMessage := func(err error) string {
			switch realErr := err.(type) {
			case nil:
				return "syntax is ok"
			case *json.SyntaxError:
				return fmt.Sprintf("%v at offset %d", realErr, realErr.Offset)
			case *os.PathError:
				return fmt.Sprintf("%v", realErr)
			default:
				return fmt.Sprintf("(%T) %v", realErr, realErr)
			}
		}(err)

		stderrLogger.Fatalf("Error in config: %s", errMessage)

	} else {
		if flags.Testconf {
			fmt.Printf("testconf %s: syntax is ok\n", configPath)
		}
	}

	mergeCommandlineFlagsToConfig(flags, config)

	daemonConfig := config.GetDaemonConfig()

	// setup unix stuff
	err = setupUnixStuff()
	if err != nil {
		log.Fatalf("setup unix stuff error: %v", err)
	}

	// antoxa: need the fancy wrapper function to have testconf behave properly
	// FIXME: testconf should check more stuff (below) and graceful restart also
	initPidfileLogfile := func() error {

		// FIXME(antoxa): this testconf thingy is everywhere! must... resist... full rewrite
		if flags.Testconf {
			err = pidfileTest(daemonConfig.GetPidFile())
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
		logLevel := daemonConfig.GetLogLevel()
		if logLevel == 0 {
			return fmt.Errorf("unknown log_level, supported: %v", badoo_config.ServiceConfigDaemonConfigTLogLevels_name)
		}
		err = reopenLogfile(daemonConfig.GetLogFile(), log.Level(logLevel))
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
			}
			return vi.GetVersion()
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

	if daemonConfig.GetEnableDebugcharts() {
		debugcharts.Enable()
	}

	// process pinba configuration and related stuff
	pinbaCtx, err = func() (*PinbaSender, error) { // assigns a global
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
	restartData, err = parseRestartDataFromEnv()
	if err != nil {
		log.Fatalf("can't parse restart data: %v", err)
	}
	if restartData != nil {
		log.Debugf("[CHILD] this is a restart, parent: %d, me: %d", restartData.PPid, os.Getpid())
	}

	// start http pprof server (possibly - inherit fd from parent if this is a restart)
	err = func() (err error) {
		httpServer, err = newHTTPServer(config, restartData) // assigning a global here
		if err != nil {
			return err
		}

		if httpServer != nil { // nil here means it has not been configured
			go httpServer.Serve()
		}

		return nil
	}()
	if err != nil {
		log.Fatalf("can't start http_pprof server: %v", err)
	}
}

// Call this when you want to start your servers and stuff
func EventLoop(addPorts []Port) {
	defer log.Debug("exiting")

	initPhaseDuration = time.Since(startupTime)

	daemonConfig := config.GetDaemonConfig()

	ports = append(ports, addPorts...)
	ports = append(ports, GpbPort("service-stats-gpb", statsCtx, badoo_service.Gpbrpc))
	ports = append(ports, JsonPort("service-stats-gpb/json", statsCtx, badoo_service.Gpbrpc))

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

	getRestartSocket := func(rcd *restartChildData, portName, portAddr string) (*restartSocket, *os.File) {
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

		// fixup settings
		port.ConfAddress = portAddr
		port.SlowRequestTime = time.Duration(daemonConfig.GetSlowRequestMs()) * time.Millisecond
		port.MaxParallelRequests = uint(daemonConfig.GetMaxParallelRequests())

		// enable pinba only for ports that explicitly request it
		port.PinbaSender = func() gpbrpc.PinbaSender {
			if !lcf.GetPinbaEnabled() {
				return nil // explicit nil here
			}

			if pinbaCtx == nil {
				log.Warnf("pinba is not configured, but pinba_enabled IS set for port %s: %s", portName, portAddr)
				return nil // explicit nil here
			}

			log.Infof("pinba configured for port %s:%s -> %s", portName, portAddr, pinbaCtx.Address)
			return pinbaCtx
		}()

		// listeners, restart sockets
		listener, err := func() (listener net.Listener, err error) { // it's important that this should be a function, see defer inside
			restartSocket, restartFile := getRestartSocket(restartData, portName, portAddr)

			// this whole fd/file affair is very inconvenient to
			//  since when getRestartSocket() returns fd - it can't close it yet, as it can be used by FileListener
			defer restartFile.Close()

			if restartSocket == nil {
				listener, err = port.Listen("tcp4", portAddr)
				if err != nil {
					return nil, fmt.Errorf("listen failed for port %s at %s: %s", portName, portAddr, err)
				}
				log.Infof("port %s bound to address %s", portName, listener.Addr())

			} else {
				listener, err = net.FileListener(restartFile) // this dup()-s
				if err != nil {
					return nil, fmt.Errorf("failed to grab parent fd %d for %s at %s: %s", restartSocket.Fd, portName, portAddr, err)
				}

				log.Infof("port %s bound to address %s (parent fd: %d)", portName, listener.Addr(), restartSocket.Fd)
			}
			return
		}()

		if err != nil {
			log.Fatalf("%v", err)
		}

		// start server, this might fail (especially zmq can)
		srv, err := port.NewServer(listener, port)
		if err != nil {
			log.Fatalf("port %s, can't start server: %v", port.Name, err)
		}

		psrv := &portServer{
			name:        port.Name,
			confAddress: port.ConfAddress,
			realAddress: srv.Listener().Addr().String(),
			port:        port,
			server:      srv,
		}
		go psrv.server.Serve()

		port.IsStarted = true
		startedServers[port.Name] = psrv // save it for laterz
	}

	// send info to mothership
	if flags.Mothership != "" {

		sendInfoToMothership := func() error {
			conn, err := net.Dial("tcp", flags.Mothership)
			if err != nil {
				return fmt.Errorf("connect: %v", err)
			}

			defer conn.Close()

			sendString := func(str string) error {
				log.Debugf("sending %q", str)
				_, err := conn.Write([]byte(fmt.Sprintf("%s\n", str)))
				return err
			}

			for _, psrv := range startedServers {
				_, netPort, _ := net.SplitHostPort(psrv.realAddress)
				err := sendString(fmt.Sprintf("%s:%s", psrv.name, netPort))
				if err != nil {
					return fmt.Errorf("failed to send port %s: %v", psrv.name, err)
				}
			}

			err = sendString(fmt.Sprintf("pid:%d", os.Getpid()))
			if err != nil {
				return fmt.Errorf("failed to send pid: %v", err)
			}

			return nil
		}

		err := sendInfoToMothership()
		if err != nil {
			log.Fatalf("sendInfoToMothership %v: %v", flags.Mothership, err)
		}
	}

	// kill parent if this is a child of graceful restart
	if restartData != nil {
		syscall.Kill(restartData.PPid, syscall.SIGQUIT)
	}

	log.Infof("entering event loop")

	exitMethod := waitForSignals()

	if exitMethod == exitGracefully {

		// run graceful in parallel
		// and wait for all of them to stop within a given time interval
		// if they can't - just exit and let them rot

		nServers := len(startedServers)
		doneChan := make(chan struct{}, nServers)

		// ask servers to stop
		for _, server := range startedServers {
			s := server // copy for goroutine
			go func() {
				if err := s.server.StopGraceful(); err != nil {
					log.Infof("failed to shutdown port %s gracefully: %s", s.name, err)
				} else {
					log.Infof("port %s stopped", s.name)
				}
				doneChan <- struct{}{}
			}()
		}

		// wait for servers to stop
		func() {
			timeoutChan := time.After(time.Duration(daemonConfig.GetParentWaitTimeout()) * time.Second)
			for {
				select {
				case <-timeoutChan:
					return
				case <-doneChan:
					nServers--
					if nServers == 0 {
						return
					}
				}
			}
		}()
	} else {
		// do nothing for EXIT_IMMEDIATELY
	}

	// doing cleanups here
	// XXX: can this be moved to defer at the start of this function?
	if pidfile != nil {
		pidfile.CloseAndRemove()
	}
}

func Hostname() string {
	return hostname
}

func DaemonConfig() *badoo_config.ServiceConfigDaemonConfigT {
	return config.GetDaemonConfig()
}

func GpbPort(name string, handler interface{}, rpc gpbrpc.Protocol) Port {
	return Port{
		Name:      name,
		Handler:   handler,
		IsStarted: false,

		Listen: func(network, address string) (net.Listener, error) {
			return net.Listen(network, address)
		},

		NewServer: func(l net.Listener, p *Port) (gpbrpc.Server, error) {
			s := gpbrpc.NewTCPServer(l, p.Handler, gpbrpc.TCPServerConfig{
				Proto:           rpc,
				Codec:           &gpbrpc.GpbsCodec,
				PinbaSender:     p.PinbaSender,
				SlowRequestTime: p.SlowRequestTime,
			})
			return s, nil
		},
	}
}

func JsonPort(name string, handler interface{}, rpc gpbrpc.Protocol) Port {
	return Port{
		Name:      name,
		Handler:   handler,
		Proto:     rpc,
		IsStarted: false,

		Listen: func(network, address string) (net.Listener, error) {
			return net.Listen(network, address)
		},

		NewServer: func(l net.Listener, p *Port) (gpbrpc.Server, error) {
			s := gpbrpc.NewTCPServer(l, p.Handler, gpbrpc.TCPServerConfig{
				Proto:           rpc,
				Codec:           &gpbrpc.JsonCodec,
				PinbaSender:     p.PinbaSender,
				SlowRequestTime: p.SlowRequestTime,
			})
			return s, nil
		},
	}
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

func GetConfigPath() string {
	return configPath
}

func GetStartedServers() map[string]*portServer {
	return startedServers
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

func mergeCommandlineFlagsToConfig(flags flagsCollection, config Config) {
	// merge the rest of command line arguments into config
	if flags.LogFile != "" {
		config.GetDaemonConfig().LogFile = proto.String(flags.LogFile)
	}

	if flags.PidFile != "" {
		config.GetDaemonConfig().PidFile = proto.String(flags.PidFile)
	}

	if flags.InstanceName != "" {
		config.GetDaemonConfig().ServiceInstanceName = proto.String(flags.InstanceName)
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
		stats, err := gatherServiceStats()
		if err != nil {
			return struct {
				Error string `json:"error"`
			}{err.Error()}
		}
		return stats
	}))
}
