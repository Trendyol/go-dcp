package kubernetes

import (
	"context"

	"github.com/Trendyol/go-dcp/models"

	"github.com/Trendyol/go-dcp/logger"

	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientSet "k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
)

type Client interface {
	CoordinationV1() v1.CoordinationV1Interface
	AddLabel(namespace string, key string, value string)
	RemoveLabel(namespace string, key string)
}

type client struct {
	myIdentity *models.Identity
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
		logger.ErrorLog.Printf("failed to add label: %v", err)
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
		logger.ErrorLog.Printf("failed to remove label: %v", err)
	}
}

func NewClient(myIdentity *models.Identity) Client {
	kubernetesConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.ErrorLog.Printf("failed to get kubernetes config: %v", err)
		panic(err)
	}

	return &client{
		myIdentity: myIdentity,
		Clientset:  clientSet.NewForConfigOrDie(kubernetesConfig),
	}
}
