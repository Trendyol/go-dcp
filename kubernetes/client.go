package kubernetes

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	v1 "k8s.io/client-go/kubernetes/typed/coordination/v1"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/logger"

	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientSet "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const DefaultNamespace = "default"

type Client interface {
	CoordinationV1() v1.CoordinationV1Interface
	AddLabel(key string, value string)
	RemoveLabel(key string)
	GetIdentity() *models.Identity
}

type client struct {
	myIdentity *models.Identity
	clientSet  *clientSet.Clientset
	namespace  string
}

func (le *client) CoordinationV1() v1.CoordinationV1Interface {
	return le.clientSet.CoordinationV1()
}

func (le *client) AddLabel(key string, value string) {
	_, err := le.clientSet.CoreV1().Pods(le.namespace).Patch(
		context.Background(),
		le.myIdentity.Name,
		types.MergePatchType, []byte(`{"metadata":{"labels":{"`+key+`":"`+value+`"}}}`),
		metaV1.PatchOptions{},
	)
	if err != nil {
		logger.Log.Error("failed to add label: %v", err)
	}
}

func (le *client) RemoveLabel(key string) {
	_, err := le.clientSet.CoreV1().Pods(le.namespace).Patch(
		context.Background(),
		le.myIdentity.Name,
		types.MergePatchType, []byte(`{"metadata":{"labels":{"`+key+`":null}}}`),
		metaV1.PatchOptions{},
	)
	if err != nil {
		logger.Log.Error("failed to remove label: %v", err)
	}
}

func getNamespace() string {
	// https://github.com/kubernetes/client-go/blob/master/tools/clientcmd/client_config.go#L582C1-L597C2
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}

	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
			return ns
		}
	}

	return DefaultNamespace
}

func (le *client) setIdentity() {
	hostname, err := os.Hostname()
	if err != nil {
		logger.Log.Error("error while getting hostname, err: %v", err)
		panic(err)
	}

	var podIP string

	if podIPFromEnv := os.Getenv("POD_IP"); podIPFromEnv != "" {
		podIP = podIPFromEnv
	} else {
		var tries int

		for {
			pod, err := le.clientSet.CoreV1().Pods(le.namespace).Get(
				context.Background(),
				hostname,
				metaV1.GetOptions{},
			)
			if err != nil {
				logger.Log.Error("error while getting pod, err: %v", err)
				panic(err)
			}

			if pod.Status.PodIP != "" {
				podIP = pod.Status.PodIP
				break
			}

			tries++

			if tries > 10 {
				err := errors.New("after 10 tries, pod ip is still empty")
				logger.Log.Error("error while get pod ip, err: %v", err)
				panic(err)
			} else {
				logger.Log.Debug("pod ip is empty, waiting...")
			}

			time.Sleep(time.Second)
		}
	}

	le.myIdentity = &models.Identity{
		IP:              podIP,
		Name:            hostname,
		ClusterJoinTime: time.Now().UnixNano(),
	}
}

func (le *client) GetIdentity() *models.Identity {
	return le.myIdentity
}

func NewClient() Client {
	kubernetesConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Log.Error("error while get kubernetes config, err: %v", err)
		panic(err)
	}

	namespace := getNamespace()

	logger.Log.Debug("kubernetes namespace: %s", namespace)

	client := &client{
		clientSet: clientSet.NewForConfigOrDie(kubernetesConfig),
		namespace: namespace,
	}

	client.setIdentity()

	return client
}
