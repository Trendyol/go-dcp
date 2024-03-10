package couchbase

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/Trendyol/go-dcp/wrapper"

	"golang.org/x/sync/errgroup"

	"github.com/Trendyol/go-dcp/helpers"
	"github.com/couchbase/gocbcore/v10/connstr"

	"github.com/Trendyol/go-dcp/config"

	"github.com/google/uuid"

	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

type Client interface {
	Ping() (*models.PingResult, error)
	GetAgent() *gocbcore.Agent
	GetMetaAgent() *gocbcore.Agent
	Connect() error
	Close()
	DcpConnect(useExpiryOpcode bool, useChangeStreams bool) error
	DcpClose()
	GetVBucketSeqNos() (*wrapper.ConcurrentSwissMap[uint16, uint64], error)
	GetNumVBuckets() int
	GetFailoverLogs(vbID uint16) ([]gocbcore.FailoverEntry, error)
	OpenStream(vbID uint16, collectionIDs map[uint32]string, offset *models.Offset, observer Observer) error
	CloseStream(vbID uint16) error
	GetCollectionIDs(scopeName string, collectionNames []string) map[uint32]string
	GetAgentConfigSnapshot() (*gocbcore.ConfigSnapshot, error)
	GetDcpAgentConfigSnapshot() (*gocbcore.ConfigSnapshot, error)
	GetAgentQueues() []*models.AgentQueue
}

type client struct {
	agent     *gocbcore.Agent
	metaAgent *gocbcore.Agent
	dcpAgent  *gocbcore.DCPAgent
	config    *config.Dcp
}

func getServiceEndpoint(result *gocbcore.PingResult, serviceType gocbcore.ServiceType) string {
	if serviceResults, ok := result.Services[serviceType]; ok {
		for _, serviceResult := range serviceResults {
			if serviceResult.Error == nil &&
				serviceResult.State == gocbcore.PingStateOK {
				return serviceResult.Endpoint
			}
		}
	}

	return ""
}

func (s *client) Ping() (*models.PingResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.HealthCheck.Timeout)
	defer cancel()

	opm := NewAsyncOp(ctx)
	errorCh := make(chan error)

	var pingResult models.PingResult

	op, err := s.agent.Ping(gocbcore.PingOptions{
		ServiceTypes: []gocbcore.ServiceType{gocbcore.MemdService, gocbcore.MgmtService},
	}, func(result *gocbcore.PingResult, err error) {
		if err == nil {
			pingResult.MemdEndpoint = getServiceEndpoint(result, gocbcore.MemdService)
			pingResult.MgmtEndpoint = getServiceEndpoint(result, gocbcore.MgmtService)
		}

		if pingResult.MemdEndpoint == "" || pingResult.MgmtEndpoint == "" {
			if err == nil {
				err = errors.New("some services are not healthy")
			}
		}

		opm.Resolve()
		errorCh <- err
	})

	err = opm.Wait(op, err)
	if err != nil {
		return nil, err
	}

	return &pingResult, <-errorCh
}

func (s *client) GetAgent() *gocbcore.Agent {
	return s.agent
}

func (s *client) GetMetaAgent() *gocbcore.Agent {
	return s.metaAgent
}

func CreateTLSRootCaProvider(rootCAPath string) func() *x509.CertPool {
	cert, err := os.ReadFile(os.ExpandEnv(rootCAPath))
	if err != nil {
		logger.Log.Error("error while reading cert file: %v", err)
		panic(err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(cert)

	return func() *x509.CertPool {
		return certPool
	}
}

func CreateSecurityConfig(username string, password string, secureConnection bool, rootCAPath string) gocbcore.SecurityConfig {
	securityConfig := gocbcore.SecurityConfig{
		Auth: gocbcore.PasswordAuthProvider{
			Username: username,
			Password: password,
		},
	}

	if secureConnection {
		securityConfig.UseTLS = true
		securityConfig.TLSRootCAProvider = CreateTLSRootCaProvider(rootCAPath)
	}

	return securityConfig
}

func CreateAgent(httpAddresses []string, bucketName string,
	username string, password string, secureConnection bool, rootCAPath string,
	connectionBufferSize uint, connectionTimeout time.Duration,
) (*gocbcore.Agent, error) {
	agent, err := gocbcore.CreateAgent(
		&gocbcore.AgentConfig{
			BucketName: bucketName,
			SeedConfig: gocbcore.SeedConfig{
				HTTPAddrs: resolveHostsAsHTTP(httpAddresses),
			},
			SecurityConfig: CreateSecurityConfig(username, password, secureConnection, rootCAPath),
			CompressionConfig: gocbcore.CompressionConfig{
				Enabled: true,
			},
			IoConfig: gocbcore.IoConfig{
				UseCollections: true,
			},
			KVConfig: gocbcore.KVConfig{
				ConnectionBufferSize: connectionBufferSize,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	ch := make(chan error)

	_, err = agent.WaitUntilReady(
		time.Now().Add(connectionTimeout),
		gocbcore.WaitUntilReadyOptions{
			RetryStrategy: gocbcore.NewBestEffortRetryStrategy(nil),
		},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			ch <- err
		},
	)
	if err != nil {
		return nil, err
	}

	if err = <-ch; err != nil {
		return nil, err
	}

	return agent, nil
}

func (s *client) connect(bucketName string, connectionBufferSize uint, connectionTimeout time.Duration) (*gocbcore.Agent, error) {
	return CreateAgent(s.config.Hosts, bucketName, s.config.Username, s.config.Password, s.config.SecureConnection, s.config.RootCAPath, connectionBufferSize, connectionTimeout) //nolint:lll
}

func resolveHostsAsHTTP(hosts []string) []string {
	if len(hosts) == 1 {
		parsedConnStr, err := connstr.Parse(hosts[0])
		if err != nil {
			panic("error parsing connection string " + hosts[0] + " " + err.Error())
		}

		out, err := connstr.Resolve(parsedConnStr)
		if err != nil {
			panic("error resolving connection string " + parsedConnStr.String() + " " + err.Error())
		}

		var httpHosts []string
		for _, specHost := range out.HttpHosts {
			httpHosts = append(httpHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
		}

		return httpHosts
	}

	return hosts
}

func (s *client) Connect() error {
	connectionBufferSize := uint(helpers.ResolveUnionIntOrStringValue(s.config.ConnectionBufferSize))
	connectionTimeout := s.config.ConnectionTimeout

	if s.config.IsCouchbaseMetadata() {
		couchbaseMetadataConfig := s.config.GetCouchbaseMetadata()
		if couchbaseMetadataConfig.Bucket == s.config.BucketName {
			if couchbaseMetadataConfig.ConnectionBufferSize > connectionBufferSize {
				connectionBufferSize = couchbaseMetadataConfig.ConnectionBufferSize
			}

			if couchbaseMetadataConfig.ConnectionTimeout > connectionTimeout {
				connectionTimeout = couchbaseMetadataConfig.ConnectionTimeout
			}
		}
	}

	agent, err := s.connect(s.config.BucketName, connectionBufferSize, connectionTimeout)
	if err != nil {
		return err
	}

	s.agent = agent

	if s.config.IsCouchbaseMetadata() {
		couchbaseMetadataConfig := s.config.GetCouchbaseMetadata()
		if couchbaseMetadataConfig.Bucket == s.config.BucketName {
			s.metaAgent = agent
		} else {
			metaAgent, err := s.connect(
				couchbaseMetadataConfig.Bucket,
				couchbaseMetadataConfig.ConnectionBufferSize,
				couchbaseMetadataConfig.ConnectionTimeout,
			)
			if err != nil {
				return err
			}

			s.metaAgent = metaAgent
		}

		logger.Log.Info("connected to %s, bucket: %s, meta bucket: %s", s.config.Hosts, s.config.BucketName, couchbaseMetadataConfig.Bucket)
		return nil
	}

	logger.Log.Info("connected to %s, bucket: %s", s.config.Hosts, s.config.BucketName)

	return nil
}

func (s *client) Close() {
	if s.metaAgent != nil {
		_ = s.metaAgent.Close()
	}

	if s.metaAgent != s.agent {
		_ = s.agent.Close()
	}

	logger.Log.Info("connections closed %s", s.config.Hosts)
}

func (s *client) DcpConnect(useExpiryOpcode bool, useChangeStreams bool) error {
	agentConfig := &gocbcore.DCPAgentConfig{
		BucketName: s.config.BucketName,
		SeedConfig: gocbcore.SeedConfig{
			HTTPAddrs: resolveHostsAsHTTP(s.config.Hosts),
		},
		SecurityConfig: CreateSecurityConfig(s.config.Username, s.config.Password, s.config.SecureConnection, s.config.RootCAPath),
		CompressionConfig: gocbcore.CompressionConfig{
			Enabled: true,
		},
		DCPConfig: gocbcore.DCPConfig{
			BufferSize:       helpers.ResolveUnionIntOrStringValue(s.config.Dcp.BufferSize),
			UseExpiryOpcode:  useExpiryOpcode,
			UseChangeStreams: useChangeStreams,
		},
		IoConfig: gocbcore.IoConfig{
			UseCollections: true,
		},
		KVConfig: gocbcore.KVConfig{
			ConnectionBufferSize: uint(helpers.ResolveUnionIntOrStringValue(s.config.Dcp.ConnectionBufferSize)),
		},
	}

	client, err := gocbcore.CreateDcpAgent(
		agentConfig,
		fmt.Sprintf("%s_%s", s.config.Dcp.Group.Name, uuid.New().String()),
		memd.DcpOpenFlagProducer,
	)
	if err != nil {
		return err
	}

	ch := make(chan error)

	_, err = client.WaitUntilReady(
		time.Now().Add(s.config.Dcp.ConnectionTimeout),
		gocbcore.WaitUntilReadyOptions{
			RetryStrategy: gocbcore.NewBestEffortRetryStrategy(nil),
		},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			ch <- err
		},
	)
	if err != nil {
		return err
	}

	if err = <-ch; err != nil {
		return err
	}

	s.dcpAgent = client
	logger.Log.Info("connected to %s as dcp, bucket: %s", s.config.Hosts, s.config.BucketName)

	return nil
}

func (s *client) GetAgentQueues() []*models.AgentQueue {
	var configSnapshots []*gocbcore.ConfigSnapshot
	var dcp *gocbcore.ConfigSnapshot

	agentConfigSnapshot, err := s.GetAgentConfigSnapshot()
	if err == nil {
		configSnapshots = append(configSnapshots, agentConfigSnapshot)
	}
	dcpAgentConfigSnapshot, err := s.GetDcpAgentConfigSnapshot()
	if err == nil {
		dcp = dcpAgentConfigSnapshot
		configSnapshots = append(configSnapshots, dcp)
	}

	clientQueue := make([]*models.AgentQueue, 0)

	for i := range configSnapshots {
		configSnapshot := configSnapshots[i]
		var isDcp bool
		if configSnapshot == dcp {
			isDcp = true
		}

		snapshot := reflect.ValueOf(configSnapshot).Elem()
		state := snapshot.FieldByName("state").Elem()
		pipelines := state.FieldByName("pipelines")
		pipelineLen := pipelines.Len()

		for i := 0; i < pipelineLen; i++ {
			pipeline := pipelines.Index(i).Elem()

			address := pipeline.FieldByName("address").String()
			queue := pipeline.FieldByName("queue").Elem()
			max := pipeline.FieldByName("maxItems").Int()
			items := queue.FieldByName("items").Elem()
			current := items.FieldByName("len").Int()

			clientQueue = append(clientQueue, &models.AgentQueue{
				Address: address,
				IsDcp:   isDcp,
				Current: int(current),
				Max:     int(max),
			})
		}
	}

	return clientQueue
}

func (s *client) DcpClose() {
	_ = s.dcpAgent.Close()
	logger.Log.Info("dcp connection closed %s", s.config.Hosts)
}

func (s *client) GetVBucketSeqNos() (*wrapper.ConcurrentSwissMap[uint16, uint64], error) {
	snapshot, err := s.GetDcpAgentConfigSnapshot()
	if err != nil {
		return nil, err
	}

	numNodes, err := snapshot.NumServers()
	if err != nil {
		return nil, err
	}

	eg := errgroup.Group{}

	seqNos := wrapper.CreateConcurrentSwissMap[uint16, uint64](1024)

	hasCollectionSupport := s.dcpAgent.HasCollectionsSupport()
	var collectionIDs []uint32
	for collectionID := range s.GetCollectionIDs(s.config.ScopeName, s.config.CollectionNames) {
		collectionIDs = append(collectionIDs, collectionID)
	}
	collectionIDSize := len(collectionIDs)
	if !hasCollectionSupport {
		collectionIDSize = 1
	}

	for i := 1; i <= numNodes; i++ {
		for j := 0; j < collectionIDSize; j++ {
			eg.Go(func(i int, j int) func() error {
				return func() error {
					opm := NewAsyncOp(context.Background())

					opts := gocbcore.GetVbucketSeqnoOptions{}
					if hasCollectionSupport {
						opts.FilterOptions = &gocbcore.GetVbucketSeqnoFilterOptions{
							CollectionID: collectionIDs[j],
						}
					}

					op, err := s.dcpAgent.GetVbucketSeqnos(
						i, memd.VbucketStateActive, opts,
						func(entries []gocbcore.VbSeqNoEntry, err error) {
							for _, entry := range entries {
								if seqNo, exist := seqNos.Load(entry.VbID); !exist || (exist && uint64(entry.SeqNo) > seqNo) {
									seqNos.Store(entry.VbID, uint64(entry.SeqNo))
								}
							}

							opm.Resolve()
						},
					)
					if err != nil {
						return err
					}
					return opm.Wait(op, err)
				}
			}(i, j))
		}
	}

	err = eg.Wait()
	if err != nil {
		return nil, err
	}

	return seqNos, nil
}

func (s *client) GetNumVBuckets() int {
	snapshot, err := s.GetDcpAgentConfigSnapshot()
	if err != nil {
		logger.Log.Error("failed to get config snapshot: %v", err)
		panic(err)
	}

	vBuckets, err := snapshot.NumVbuckets()
	if err != nil {
		logger.Log.Error("failed to get number of vbucket: %v", err)
		panic(err)
	}

	return vBuckets
}

func (s *client) GetAgentConfigSnapshot() (*gocbcore.ConfigSnapshot, error) { //nolint:unused
	return s.agent.ConfigSnapshot()
}

func (s *client) GetDcpAgentConfigSnapshot() (*gocbcore.ConfigSnapshot, error) { //nolint:unused
	return s.dcpAgent.ConfigSnapshot()
}

func (s *client) GetFailoverLogs(vbID uint16) ([]gocbcore.FailoverEntry, error) {
	opm := NewAsyncOp(context.Background())
	ch := make(chan error)

	var failoverLogs []gocbcore.FailoverEntry

	op, err := s.dcpAgent.GetFailoverLog(
		vbID,
		func(entries []gocbcore.FailoverEntry, err error) {
			failoverLogs = entries

			opm.Resolve()

			ch <- err
		})

	err = opm.Wait(op, err)
	if err != nil {
		return nil, err
	}

	return failoverLogs, <-ch
}

func (s *client) openStreamWithRollback(vbID uint16,
	failedSeqNo gocbcore.SeqNo,
	rollbackSeqNo gocbcore.SeqNo,
	observer Observer,
	openStreamOptions gocbcore.OpenStreamOptions,
) error {
	logger.Log.Info(
		"open stream with rollback, vbID: %d, failedSeqNo: %d, rollbackSeqNo: %d",
		vbID, failedSeqNo, rollbackSeqNo,
	)

	opm := NewAsyncOp(context.Background())

	ch := make(chan error)

	op, err := s.dcpAgent.OpenStream(
		vbID,
		0,
		0,
		rollbackSeqNo,
		0xffffffffffffffff,
		rollbackSeqNo,
		rollbackSeqNo,
		observer,
		openStreamOptions,
		func(failoverLogs []gocbcore.FailoverEntry, err error) {
			if err == nil {
				observer.SetVbUUID(vbID, failoverLogs[0].VbUUID)
				observer.AddCatchup(vbID, failedSeqNo)
			}

			opm.Resolve()

			ch <- err
		},
	)

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	return <-ch
}

func (s *client) OpenStream(
	vbID uint16,
	collectionIDs map[uint32]string,
	offset *models.Offset,
	observer Observer,
) error {
	opm := NewAsyncOp(context.Background())

	openStreamOptions := gocbcore.OpenStreamOptions{}

	if s.dcpAgent.HasCollectionsSupport() {
		openStreamOptions.ManifestOptions = &gocbcore.OpenStreamManifestOptions{ManifestUID: 0}

		options := &gocbcore.OpenStreamFilterOptions{
			CollectionIDs: []uint32{},
		}

		for id := range collectionIDs {
			options.CollectionIDs = append(options.CollectionIDs, id)
		}

		if len(options.CollectionIDs) > 0 {
			openStreamOptions.FilterOptions = options
		}
	}

	ch := make(chan error)

	op, err := s.dcpAgent.OpenStream(
		vbID,
		0,
		offset.VbUUID,
		gocbcore.SeqNo(offset.SeqNo),
		0xffffffffffffffff,
		gocbcore.SeqNo(offset.StartSeqNo),
		gocbcore.SeqNo(offset.EndSeqNo),
		observer,
		openStreamOptions,
		func(failoverLogs []gocbcore.FailoverEntry, err error) {
			if err == nil {
				observer.SetVbUUID(vbID, failoverLogs[0].VbUUID)
			}

			opm.Resolve()

			ch <- err
		},
	)

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	err = <-ch
	if err != nil {
		if rollbackErr, ok := err.(gocbcore.DCPRollbackError); ok {
			logger.Log.Info("need to rollback for vbID: %d, vbUUID: %d", vbID, offset.VbUUID)
			return s.openStreamWithRollback(vbID, gocbcore.SeqNo(offset.SeqNo), rollbackErr.SeqNo, observer, openStreamOptions)
		}
	}

	return err
}

func (s *client) CloseStream(vbID uint16) error {
	opm := NewAsyncOp(context.Background())

	ch := make(chan error)

	op, err := s.dcpAgent.CloseStream(
		vbID,
		gocbcore.CloseStreamOptions{},
		func(err error) {
			opm.Resolve()

			ch <- err
		},
	)

	err = opm.Wait(op, err)
	if err != nil {
		return err
	}

	return <-ch
}

func (s *client) getCollectionID(scopeName string, collectionName string) (uint32, error) {
	opm := NewAsyncOp(context.Background())

	ch := make(chan error)
	var collectionID uint32
	op, err := s.agent.GetCollectionID(
		scopeName,
		collectionName,
		gocbcore.GetCollectionIDOptions{},
		func(result *gocbcore.GetCollectionIDResult, err error) {
			if err == nil {
				collectionID = result.CollectionID
			}

			opm.Resolve()

			ch <- err
		},
	)
	err = opm.Wait(op, err)
	if err != nil {
		return collectionID, err
	}

	return collectionID, <-ch
}

func (s *client) GetCollectionIDs(scopeName string, collectionNames []string) map[uint32]string {
	collectionIDs := map[uint32]string{}

	if s.dcpAgent.HasCollectionsSupport() {
		for _, collectionName := range collectionNames {
			collectionID, err := s.getCollectionID(scopeName, collectionName)
			if err != nil {
				logger.Log.Error("cannot get collection ids: %v", err)
				panic(err)
			}

			collectionIDs[collectionID] = collectionName
		}
	}

	return collectionIDs
}

func NewClient(config *config.Dcp) Client {
	return &client{
		agent:    nil,
		dcpAgent: nil,
		config:   config,
	}
}
