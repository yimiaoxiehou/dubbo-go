package protocol

import (
	"github.com/dubbo/dubbo-go/protocol/protocolwrapper"
	"sync"
)

import (
	log "github.com/AlexStocks/log4go"
	jerrors "github.com/juju/errors"
)

import (
	"github.com/dubbo/dubbo-go/common/constant"
	"github.com/dubbo/dubbo-go/common/extension"
	"github.com/dubbo/dubbo-go/config"
	"github.com/dubbo/dubbo-go/protocol"
	"github.com/dubbo/dubbo-go/registry"
	directory2 "github.com/dubbo/dubbo-go/registry/directory"
)

const RegistryConnDelay = 3

var registryProtocol *RegistryProtocol

type RegistryProtocol struct {
	// Registry  Map<RegistryAddress, Registry>
	registies sync.Map
}

func init() {
	extension.SetProtocol("registry", GetProtocol)
}

func NewRegistryProtocol() *RegistryProtocol {
	return &RegistryProtocol{
		registies: sync.Map{},
	}
}
func getRegistry(regUrl *config.RegistryURL) registry.Registry {
	reg, err := extension.GetRegistryExtension(regUrl.Protocol, regUrl)
	if err != nil {
		log.Error("Registry can not connect success, program is going to panic.Error message is %s", err.Error())
		panic(err.Error())
	}
	return reg
}
func (protocol *RegistryProtocol) Refer(url config.IURL) protocol.Invoker {
	var regUrl = url.(*config.RegistryURL)
	var serviceUrl = regUrl.URL

	var reg registry.Registry

	regI, _ := protocol.registies.LoadOrStore(url.Key(),
		getRegistry(regUrl))
	reg = regI.(registry.Registry)

	//new registry directory for store service url from registry
	directory := directory2.NewRegistryDirectory(regUrl, reg)
	go directory.Subscribe(serviceUrl)

	//new cluster invoker
	cluster := extension.GetCluster(serviceUrl.Params.Get(constant.CLUSTER_KEY))
	return cluster.Join(directory)
}

func (protocol *RegistryProtocol) Export(invoker protocol.Invoker) protocol.Exporter {
	registryUrl := protocol.getRegistryUrl(invoker)
	providerUrl := protocol.getProviderUrl(invoker)

	regI, _ := protocol.registies.LoadOrStore(providerUrl.Key(),
		getRegistry(&registryUrl))

	reg := regI.(registry.Registry)

	err := reg.Register(providerUrl)
	if err != nil {
		log.Error("provider service %v register registry %v error, error message is %v", providerUrl.ToFullString(), registryUrl.String(), err.Error())
	}

	wrappedInvoker := newWrappedInvoker(invoker, providerUrl)

	return extension.GetProtocolExtension(protocolwrapper.FILTER).Export(wrappedInvoker)

}

func (*RegistryProtocol) Destroy() {
}

func (*RegistryProtocol) getRegistryUrl(invoker protocol.Invoker) config.RegistryURL {
	//here add * for return a new url
	url := *invoker.GetUrl().(*config.RegistryURL)
	//if the protocol == registry ,set protocol the registry value in url.params
	if url.Protocol == constant.REGISTRY_PROTOCOL {
		protocol := url.GetParam(constant.REGISTRY_KEY, constant.DEFAULT_PROTOCOL)
		url.Protocol = protocol
	}
	return url
}

func (*RegistryProtocol) getProviderUrl(invoker protocol.Invoker) config.URL {
	url := invoker.GetUrl().(*config.RegistryURL)
	var export string
	if export = url.GetParam(constant.EXPORT_KEY, ""); export == "" {
		err := jerrors.Errorf("The registry export url is null! registry: %v", url.String())
		log.Error(err.Error())
		panic(err)
	}
	newUrl, err := config.NewURL(url.Context(), export)
	if err != nil {
		err := jerrors.Errorf("The registry export url is invalid! registry: %v ,error messsage:%v ", url.String(), err.Error())
		log.Error(err.Error())
		panic(err)
	}
	return *newUrl
}

func GetProtocol() protocol.Protocol {
	if registryProtocol != nil {
		return registryProtocol
	}
	return NewRegistryProtocol()
}

type wrappedInvoker struct {
	invoker protocol.Invoker
	url     config.URL
	protocol.BaseInvoker
}

func newWrappedInvoker(invoker protocol.Invoker, url config.URL) *wrappedInvoker {
	return &wrappedInvoker{
		invoker:     invoker,
		url:         url,
		BaseInvoker: *protocol.NewBaseInvoker(nil),
	}
}
func (ivk *wrappedInvoker) GetUrl() config.IURL {
	return &ivk.url
}
func (ivk *wrappedInvoker) getInvoker() protocol.Invoker {
	return ivk.invoker
}
