package cdc

import (
	"context"
	goerrors "errors"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/http"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/go-playground/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type Connector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	Close()
	GetConfig() *config.Config
	SetMetricCollectors(collectors ...prometheus.Collector)
}

type connector struct {
	stream             replication.Streamer
	prometheusRegistry metric.Registry
	server             http.Server
	cfg                *config.Config
	slot               *slot.Slot
	cancelCh           chan os.Signal
	readyCh            chan struct{}
	system             pq.IdentifySystemResult

	once sync.Once
}

func NewConnectorWithConfigFile(ctx context.Context, configFilePath string, listenerFunc replication.ListenerFunc) (Connector, error) {
	var cfg config.Config
	var err error

	if strings.HasSuffix(configFilePath, ".json") {
		cfg, err = config.ReadConfigJSON(configFilePath)
	}

	if strings.HasSuffix(configFilePath, ".yml") || strings.HasSuffix(configFilePath, ".yaml") {
		cfg, err = config.ReadConfigYAML(configFilePath)
	}

	if err != nil {
		return nil, err
	}

	return NewConnector(ctx, cfg, listenerFunc)
}

func NewConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	logger.InitLogger(cfg.Logger.Logger)
 
	logger.Debug("NewConnector: starting config setup")
	cfg.SetDefault()
	logger.Debug("NewConnector: config defaults set")
	
	if err := cfg.Validate(); err != nil {
		logger.Debug("NewConnector: config validation failed", "error", err)
		return nil, errors.Wrap(err, "config validation")
	}
	logger.Debug("NewConnector: config validated successfully")
	
	cfg.Print()
	logger.Debug("NewConnector: config printed")

	
	logger.Debug("NewConnector: initializing logger")

	logger.Debug("NewConnector: creating new connection")
	conn, err := pq.NewConnection(ctx, cfg.DSN())
	if err != nil {
		logger.Debug("NewConnector: connection creation failed", "error", err)
		return nil, err
	}
	logger.Debug("NewConnector: connection created successfully")

	logger.Debug("NewConnector: creating publication")
	pub := publication.New(cfg.Publication, conn)
	logger.Debug("NewConnector: publication created, setting replica identities")
	
	if err = pub.SetReplicaIdentities(ctx); err != nil {
		logger.Debug("NewConnector: setting replica identities failed", "error", err)
		return nil, err
	}
	logger.Debug("NewConnector: replica identities set, creating publication")
	
	publicationInfo, err := pub.Create(ctx)
	if err != nil {
		logger.Debug("NewConnector: publication creation failed", "error", err)
		return nil, err
	}
	logger.Debug("NewConnector: publication created successfully")
	logger.Info("publication", "info", publicationInfo)

	logger.Debug("NewConnector: identifying system")
	system, err := pq.IdentifySystem(ctx, conn)
	if err != nil {
		logger.Debug("NewConnector: system identification failed", "error", err)
		return nil, err
	}
	logger.Debug("NewConnector: system identified successfully")
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database:", system.Database)

	logger.Debug("NewConnector: creating metric")
	m := metric.NewMetric(cfg.Slot.Name)
	logger.Debug("NewConnector: metric created")

	logger.Debug("NewConnector: creating replication stream")
	stream := replication.NewStream(conn, cfg, m, &system, listenerFunc)
	logger.Debug("NewConnector: replication stream created")

	logger.Debug("NewConnector: creating slot")
	sl, err := slot.NewSlot(ctx, cfg.DSN(), cfg.Slot, m, stream.(slot.XLogUpdater))
	if err != nil {
		logger.Debug("NewConnector: slot creation failed", "error", err)
		return nil, err
	}
	logger.Debug("NewConnector: slot created, creating slot info")

	slotInfo, err := sl.Create(ctx)
	if err != nil {
		logger.Debug("NewConnector: slot info creation failed", "error", err)
		return nil, err
	}
	logger.Debug("NewConnector: slot info created successfully")
	logger.Info("slot info", "info", slotInfo)

	logger.Debug("NewConnector: creating prometheus registry")
	prometheusRegistry := metric.NewRegistry(m)
	logger.Debug("NewConnector: prometheus registry created")

	logger.Debug("NewConnector: creating connector struct")
	connector := &connector{
		cfg:                &cfg,
		system:             system,
		stream:             stream,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry),
		slot:               sl,

		cancelCh: make(chan os.Signal, 1),
		readyCh:  make(chan struct{}, 1),
	}
	logger.Debug("NewConnector: connector created successfully")

	return connector, nil
}

func (c *connector) Start(ctx context.Context) {
	c.once.Do(func() {
		go c.server.Listen()
	})

	c.CaptureSlot(ctx)

	err := c.stream.Open(ctx)
	if err != nil {
		if goerrors.Is(err, replication.ErrorSlotInUse) {
			logger.Info("capture failed")
			c.Start(ctx)
			return
		}
		logger.Error("postgres stream open", "error", err)
		return
	}

	logger.Info("slot captured")
	go c.slot.Metrics(ctx)

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	c.readyCh <- struct{}{}

	<-c.cancelCh
	logger.Debug("cancel channel triggered")
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connector) Close() {
	if !isClosed(c.cancelCh) {
		close(c.cancelCh)
	}
	if !isClosed(c.readyCh) {
		close(c.readyCh)
	}

	c.slot.Close()
	c.stream.Close(context.TODO())
	c.server.Shutdown()
}

func (c *connector) GetConfig() *config.Config {
	return c.cfg
}

func (c *connector) SetMetricCollectors(metricCollectors ...prometheus.Collector) {
	c.prometheusRegistry.AddMetricCollectors(metricCollectors...)
}

func (c *connector) CaptureSlot(ctx context.Context) {
	logger.Info("slot capturing...")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		info, err := c.slot.Info(ctx)
		if err != nil {
			continue
		}

		if !info.Active {
			break
		}

		logger.Debug("capture slot", "slotInfo", info)
	}
}

func isClosed[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}
