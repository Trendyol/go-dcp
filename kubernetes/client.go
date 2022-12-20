package kubernetes

import (
	"context"

	"github.com/Trendyol/go-dcp-client/logger"

	dcpModel "github.com/Trendyol/go-dcp-client/identity"

	"github.com/go-logr/logr"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientSet "k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

type Client interface {
	CoordinationV1() v1.CoordinationV1Interface
	AddLabel(namespace string, key string, value string)
	RemoveLabel(namespace string, key string)
}

type client struct {
	myIdentity *dcpModel.Identity
	*clientSet.Clientset
}

func (le *client) AddLabel(namespace string, key string, value string) {
	_, err := le.CoreV1().Pods(namespace).Patch(
		context.Background(),
		le.myIdentity.Name,
		types.MergePatchType, []byte(`{"metadata":{"labels":{"`+key+`":"`+value+`"}}}`),
		metaV1.PatchOptions{},
	)
	if err != nil {
		logger.Panic(err, "failed to add label")
	}
}

func (le *client) RemoveLabel(namespace string, key string) {
	_, err := le.CoreV1().Pods(namespace).Patch(
		context.Background(),
		le.myIdentity.Name,
		types.MergePatchType, []byte(`{"metadata":{"labels":{"`+key+`":null}}}`),
		metaV1.PatchOptions{},
	)
	if err != nil {
		logger.Panic(err, "failed to remove label")
	}
}

func NewClient(myIdentity *dcpModel.Identity) Client {
	klog.SetLogger(logr.Discard())

	kubernetesConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Panic(err, "failed to get kubernetes config")
	}

	return &client{
		myIdentity: myIdentity,
		Clientset:  clientSet.NewForConfigOrDie(kubernetesConfig),
	}
}
