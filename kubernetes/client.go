package kubernetes

import (
	"context"
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
	AddLabel(key string, value string)
	RemoveLabel(key string)
}

type client struct {
	myIdentity *dcpModel.Identity
	namespace  string
	*clientSet.Clientset
}

func (le *client) AddLabel(key string, value string) {
	_, err := le.CoreV1().Pods(le.namespace).Patch(
		context.Background(),
		le.myIdentity.Name,
		types.MergePatchType, []byte(`{"metadata":{"labels":{"`+key+`":"`+value+`"}}}`),
		metaV1.PatchOptions{},
	)
	if err != nil {
		panic(err)
	}
}

func (le *client) RemoveLabel(key string) {
	_, err := le.CoreV1().Pods(le.namespace).Patch(
		context.Background(),
		le.myIdentity.Name,
		types.MergePatchType, []byte(`{"metadata":{"labels":{"`+key+`":null}}}`),
		metaV1.PatchOptions{},
	)
	if err != nil {
		panic(err)
	}
}

func NewClient(myIdentity *dcpModel.Identity, namespace string) Client {
	klog.SetLogger(logr.Discard())

	kubernetesConfig, err := rest.InClusterConfig()
	if err != nil {
		panic(err)
	}

	return &client{
		myIdentity: myIdentity,
		namespace:  namespace,
		Clientset:  clientSet.NewForConfigOrDie(kubernetesConfig),
	}
}
