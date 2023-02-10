/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package consul

import (
	"dubbo.apache.org/dubbo-go/v3/remoting/zookeeper/curator_discovery"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

import (
	gxset "github.com/dubbogo/gost/container/set"
	gxpage "github.com/dubbogo/gost/hash/page"
	"github.com/dubbogo/gost/log/logger"
	capi "github.com/hashicorp/consul/api"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/extension"
	"dubbo.apache.org/dubbo-go/v3/registry"
	"dubbo.apache.org/dubbo-go/v3/remoting"
)

const (
	rootPath = "/services"
)

func init() {
	extension.SetServiceDiscovery("consul", newConsulServiceDiscovery)
}

type consulServiceDiscovery struct {
	client              *capi.Client
	url                 *common.URL
	wg                  sync.WaitGroup
	cltLock             sync.Mutex
	listenLock          sync.Mutex
	done                chan struct{}
	rootPath            string
	listenNames         []string
	instanceListenerMap map[string]*gxset.HashSet
}

// newConsulServiceDiscovery the constructor of newConsulServiceDiscovery
func newConsulServiceDiscovery(url *common.URL) (registry.ServiceDiscovery, error) {
	consulsd := &consulServiceDiscovery{
		url:                 url,
		rootPath:            rootPath,
		instanceListenerMap: make(map[string]*gxset.HashSet),
	}
	config := capi.DefaultConfig()
	config.Address = url.Ip + url.Port
	client, err := capi.NewClient(config)
	if err != nil {
		return nil, err
	}
	consulsd.client = client
	return consulsd, nil
}

// nolint
func (consulsd *consulServiceDiscovery) ConsulClient() *capi.Client {
	return consulsd.client
}

// nolint
func (consulsd *consulServiceDiscovery) SetconsulClient(client *capi.Client) {
	consulsd.client = client
}

// nolint
func (consulsd *consulServiceDiscovery) consulClientLock() *sync.Mutex {
	return &consulsd.cltLock
}

// nolint
func (consulsd *consulServiceDiscovery) WaitGroup() *sync.WaitGroup {
	return &consulsd.wg
}

// nolint
func (consulsd *consulServiceDiscovery) Done() chan struct{} {
	return consulsd.done
}

// RestartCallBack when consul connection reconnect this function will be invoked.
// try to re-register service, and listen services
func (consulsd *consulServiceDiscovery) RestartCallBack() bool {
	//consulsd.csd.ReRegisterServices()
	//consulsd.listenLock.Lock()
	//defer consulsd.listenLock.Unlock()
	//for _, name := range consulsd.listenNames {
	//	consulsd.csd.ListenServiceEvent(name, consulsd)
	//}
	return true
}

// nolint
func (consulsd *consulServiceDiscovery) GetURL() *common.URL {
	return consulsd.url
}

// nolint
func (consulsd *consulServiceDiscovery) String() string {
	return fmt.Sprintf("consul-service-discovery[%s]", consulsd.url)
}

// Destroy will destroy the clinet.
func (consulsd *consulServiceDiscovery) Destroy() error {
	//consulsd.client.Connect().
	return nil
}

// Register will register service in consul, instance convert to curator's service instance
// which define in curator-x-discovery.
func (consulsd *consulServiceDiscovery) Register(instance registry.ServiceInstance) error {
	cris := consulsd.toCuratorInstance(instance)
	return consulsd.ConsulClient().Agent().ServiceRegister(cris)
}

// Update will update service in consul, instance convert to curator's service instance
// which define in curator-x-discovery, please refer to https://github.com/apache/curator.
func (consulsd *consulServiceDiscovery) Update(instance registry.ServiceInstance) error {
	cris := consulsd.toCuratorInstance(instance)
	return consulsd.ConsulClient().Agent().ServiceRegister(cris)
}

// Unregister will unregister the instance in consul
func (consulsd *consulServiceDiscovery) Unregister(instance registry.ServiceInstance) error {
	return consulsd.ConsulClient().Agent().ServiceDeregister(instance.GetID())
}

// GetDefaultPageSize will return the constant registry.DefaultPageSize
func (consulsd *consulServiceDiscovery) GetDefaultPageSize() int {
	return registry.DefaultPageSize
}

// GetServices will return the all services in consul
func (consulsd *consulServiceDiscovery) GetServices() *gxset.HashSet {
	services, err := consulsd.ConsulClient().Agent().Services()
	res := gxset.NewSet()
	if err != nil {
		logger.Errorf("[consulServiceDiscovery] Could not query the services: %v", err)
		return res
	}
	for _, service := range services {
		res.Add(service)
	}
	return res
}

// GetInstances will return the instances in a service
func (consulsd *consulServiceDiscovery) GetInstances(serviceName string) []registry.ServiceInstance {
	_, instances, err := consulsd.ConsulClient().Agent().AgentHealthServiceByName(serviceName)
	if err != nil {
		logger.Errorf("[consulServiceDiscovery] Could not query the instances for service{%s}, error = err{%v} ",
			serviceName, err)
		return make([]registry.ServiceInstance, 0)
	}
	iss := make([]registry.ServiceInstance, 0, len(instances))
	for _, cris := range instances {
		iss = append(iss, toconsulInstance(cris))
	}
	return iss
}

// GetInstancesByPage will return the instances
func (consulsd *consulServiceDiscovery) GetInstancesByPage(serviceName string, offset int, pageSize int) gxpage.Pager {
	all := consulsd.GetInstances(serviceName)
	res := make([]interface{}, 0, pageSize)
	// could not use res = all[a:b] here because the res should be []interface{}, not []ServiceInstance
	for i := offset; i < len(all) && i < offset+pageSize; i++ {
		res = append(res, all[i])
	}
	return gxpage.NewPage(offset, pageSize, res, len(all))
}

// GetHealthyInstancesByPage will return the instance
// In consul, all service instance's is healthy.
// However, the healthy parameter in this method maybe false. So we can not use that API.
// Thus, we must query all instances and then do filter
func (consulsd *consulServiceDiscovery) GetHealthyInstancesByPage(serviceName string, offset int, pageSize int, healthy bool) gxpage.Pager {
	all := consulsd.GetInstances(serviceName)
	res := make([]interface{}, 0, pageSize)
	// could not use res = all[a:b] here because the res should be []interface{}, not []ServiceInstance
	var (
		i     = offset
		count = 0
	)
	for i < len(all) && count < pageSize {
		ins := all[i]
		if ins.IsHealthy() == healthy {
			res = append(res, all[i])
			count++
		}
		i++
	}
	return gxpage.NewPage(offset, pageSize, res, len(all))
}

// GetRequestInstances will return the instances
func (consulsd *consulServiceDiscovery) GetRequestInstances(serviceNames []string, offset int, requestedSize int) map[string]gxpage.Pager {
	res := make(map[string]gxpage.Pager, len(serviceNames))
	for _, name := range serviceNames {
		res[name] = consulsd.GetInstancesByPage(name, offset, requestedSize)
	}
	return res
}

// AddListener ListenServiceEvent will add a data listener in service
func (consulsd *consulServiceDiscovery) AddListener(listener registry.ServiceInstancesChangedListener) error {
	consulsd.listenLock.Lock()
	defer consulsd.listenLock.Unlock()

	for _, t := range listener.GetServiceNames().Values() {
		serviceName, ok := t.(string)
		if !ok {
			logger.Errorf("service name error %s", t)
			continue
		}
		consulsd.listenNames = append(consulsd.listenNames, serviceName)
		listenerSet, found := consulsd.instanceListenerMap[serviceName]
		if !found {
			listenerSet = gxset.NewSet(listener)
			listenerSet.Add(listener)
			consulsd.instanceListenerMap[serviceName] = listenerSet
		} else {
			listenerSet.Add(listener)
		}
	}

	for _, t := range listener.GetServiceNames().Values() {
		serviceName, ok := t.(string)
		if !ok {
			logger.Errorf("service name error %s", t)
			continue
		}
		consulsd.csd.ListenServiceEvent(serviceName, consulsd)
	}
	return nil
}

// DataChange implement DataListener's DataChange function
// to resolve event to do DispatchEventByServiceName
func (consulsd *consulServiceDiscovery) DataChange(eventType remoting.Event) bool {
	path := strings.TrimPrefix(eventType.Path, consulsd.rootPath)
	path = strings.TrimPrefix(path, constant.PathSeparator)
	// get service name in consul path
	serviceName := strings.Split(path, constant.PathSeparator)[0]

	var err error
	instances := consulsd.GetInstances(serviceName)
	for _, lis := range consulsd.instanceListenerMap[serviceName].Values() {
		var instanceListener registry.ServiceInstancesChangedListener
		instanceListener = lis.(registry.ServiceInstancesChangedListener)
		err = instanceListener.OnEvent(registry.NewServiceInstancesChangedEvent(serviceName, instances))
	}

	if err != nil {
		logger.Errorf("[consulServiceDiscovery] DispatchEventByServiceName{%s} error = err{%v}", serviceName, err)
		return false
	}
	return true
}

// toCuratorInstance convert to curator's service instance
func (consulsd *consulServiceDiscovery) toCuratorInstance(instance registry.ServiceInstance) *curator_discovery.ServiceInstance {
	id := instance.GetHost() + ":" + strconv.Itoa(instance.GetPort())
	pl := make(map[string]interface{}, 8)
	pl["id"] = id
	pl["name"] = instance.GetServiceName()
	pl["metadata"] = instance.GetMetadata()
	pl["@class"] = "org.apache.dubbo.registry.consul.consulInstance"
	cuis := &curator_discovery.ServiceInstance{
		Name:                instance.GetServiceName(),
		ID:                  id,
		Address:             instance.GetHost(),
		Port:                instance.GetPort(),
		Payload:             pl,
		RegistrationTimeUTC: 0,
	}
	return cuis
}

// toconsulInstance convert to registry's service instance
func toconsulInstance(cris capi.AgentServiceChecksInfo) registry.ServiceInstance {
	pl, ok := cris.Payload.(map[string]interface{})
	if !ok {
		logger.Errorf("[consulServiceDiscovery] toconsulInstance{%s} payload is not map[string]interface{}", cris.ID)
		return nil
	}
	mdi, ok := pl["metadata"].(map[string]interface{})
	if !ok {
		logger.Errorf("[consulServiceDiscovery] toconsulInstance{%s} metadata is not map[string]interface{}", cris.ID)
		return nil
	}
	md := make(map[string]string, len(mdi))
	for k, v := range mdi {
		md[k] = fmt.Sprint(v)
	}
	return &registry.DefaultServiceInstance{
		ID:          cris.ID,
		ServiceName: cris.Name,
		Host:        cris.Address,
		Port:        cris.Port,
		Enable:      true,
		Healthy:     true,
		Metadata:    md,
	}
}
