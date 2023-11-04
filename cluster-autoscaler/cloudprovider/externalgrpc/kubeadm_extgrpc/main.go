/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"strings"

	"google.golang.org/grpc"
        "google.golang.org/grpc/codes"
        "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"golang.org/x/crypto/ssh"

	apiv1 "k8s.io/api/core/v1"
        "k8s.io/apimachinery/pkg/api/resource"
        "k8s.io/autoscaler/cluster-autoscaler/config"
//        "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

        "k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	errors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/externalgrpc/protos"
	kube_flag "k8s.io/component-base/cli/flag"
	klog "k8s.io/klog/v2"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

type MultiStringFlag []string
func (flag *MultiStringFlag) String() string {
	return "[" + strings.Join(*flag, " ") + "]"
}
func (flag *MultiStringFlag) Set(value string) error {
	*flag = append(*flag, value)
	return nil
}
func multiStringFlag(name string, usage string) *MultiStringFlag {
	value := new(MultiStringFlag)
	flag.Var(value, name, usage)
	return value
}
var (
	address        = flag.String("address", ":8086", "The address to expose the grpc service.")
	cloudConfig    = flag.String("cloud-config", "", "The path to the cloud provider configuration file.  Empty string for no configuration file.")
	clusterName    = flag.String("cluster-name", "", "Autoscaled cluster name, if available")
	nodeGroupsFlag = multiStringFlag( "nodes",
		"sets min,max size and other configuration data for a node group in a format accepted by cloud provider. Can be used multiple times. Format: <min>:<max>:<other...>")
	apiAddress    = flag.String("api-address", "127.0.0.1:6443", "The address where API server listens")
	token         = flag.String("token", "", "Token for node to join the kubeadm cluster")
	certHash      = flag.String("cert-hash", "", "discovery token ca cert hash for node to join the kubeadm cluster")
)
func debug(req fmt.Stringer) {
	klog.V(5).Info("DBG: Got gRPC request: %T %s\n", req, req)
}
func debug_warn(req fmt.Stringer) {
	klog.Warningf("DBG: Got gRPC request: %T %s\n", req, req)
}

type KubeadmNode struct {
	nodeName string
	nodeIPv4 string
}

type KubeadmNodeGroup struct {
	id      string // this must be a stable identifier
        minSize int
        maxSize int
	currentSize int
	nodeInfo *schedulerframework.NodeInfo
	nodes []KubeadmNode
	masterAddress string
	token string
	certHash string
}
func NewKubeadmNodeGroup(masterAddr, token, certHash string) cloudprovider.NodeGroup {
	kubeadmNodes := []KubeadmNode{
		{
			nodeName: "node1",
			nodeIPv4: "192.168.105.16",
		},
		{
			nodeName: "node2",
			nodeIPv4: "192.168.105.17",
		},
	}
	ni := schedulerframework.NewNodeInfo()
	return &KubeadmNodeGroup{
		id: "vDemoNodeGroup",
		minSize: 1,
		maxSize: 2,
		currentSize: 1,
		nodeInfo: ni,
		nodes: kubeadmNodes,
		masterAddress: masterAddr,
		token: token,
		certHash: certHash,
	}
}
func (n *KubeadmNodeGroup) Id() string {
	klog.V(5).Info("DBG: KubeadmNodeGroup.Id()")
        return n.id
}
func (n *KubeadmNodeGroup) Debug() string {
	klog.V(5).Info("DBG: KubeadmNodeGroup.Debug()")
        return "vDemoNodeGroupDebug"
}
func (n *KubeadmNodeGroup) MaxSize() int {
        return n.maxSize
}
func (n *KubeadmNodeGroup) MinSize() int {
        return n.minSize
}
func (n *KubeadmNodeGroup) Autoprovisioned() bool {
	klog.V(5).Info("DBG: KubeadmNodeGroup.Autoprovisioned()")
        return false
}
func (n *KubeadmNodeGroup) Exist() bool {
	klog.V(5).Info("DBG: KubeadmNodeGroup.Exist()")
        return true
}
func (n *KubeadmNodeGroup) Create() (cloudprovider.NodeGroup, error) {
	klog.V(5).Info("DBG: KubeadmNodeGroup.Create()")
        return nil, cloudprovider.ErrNotImplemented
}
func (n *KubeadmNodeGroup) Delete() error {
	klog.V(5).Info("DBG: KubeadmNodeGroup.Delete()")
        return cloudprovider.ErrNotImplemented
}
func (n *KubeadmNodeGroup) TargetSize() (int, error) {
	klog.V(5).Infof("DBG: KubeadmNodeGroup.TargetSize() CURRENT=%d\n", n.currentSize)
        return n.currentSize, nil
}
func runSshCommand(addr, cmd string) error {
    klog.V(1).Infof("SSH: ADDR='%s' CMD='%s'\n", addr, cmd)
    sshConfig := &ssh.ClientConfig{
        User: "root",
	Auth: []ssh.AuthMethod{
		ssh.Password("p"),
	},
        HostKeyCallback: ssh.InsecureIgnoreHostKey(),
    }
    conn, err := ssh.Dial("tcp", addr, sshConfig)
    if err != nil {
        klog.Errorf("Failed to connect to remote system: %v\n", err)
	return err
    }
    defer conn.Close()
    session, err := conn.NewSession()
    if err != nil {
        klog.Errorf("Failed to create SSH session: %v\n", err)
	return err
    }
    defer session.Close()
    output, err := session.CombinedOutput(cmd)
    if err != nil {
        klog.Errorf("Failed to run command on remote system: %v\n", err)
	return err
    }
    klog.Info(string(output))
    return nil
}
func (n *KubeadmNodeGroup) joinNextNode() error {
	i := 0
	var nn KubeadmNode 
	klog.V(1).Infof("DBG:  KubeadmNodeGroup.joinNextNode()\n")
	for i, nn = range n.nodes {
		if i >= n.currentSize {
			break
		}
		klog.V(4).Infof("Node # %d: Name: %s , IP: %s\n", i+1, nn.nodeName, nn.nodeIPv4)
	}
	klog.V(1).Infof("Joining Node # %d: Name: %s , IP: %s\n", i+1, n.nodes[i].nodeName, n.nodes[i].nodeIPv4)
	nodeAddr := n.nodes[i].nodeIPv4 +":22"
	cmd := "kubeadm join " + n.masterAddress + " --token " + n.token + " --discovery-token-ca-cert-hash " + n.certHash
	return runSshCommand(nodeAddr, cmd)
}
func (n *KubeadmNodeGroup) IncreaseSize(delta int) error {
	klog.V(1).Infof("DBG: KubeadmNodeGroup.IncreaseSize() CURRENT=%d DELTA=%d\n", n.currentSize, delta)
	if n.currentSize + delta < n.maxSize {
		// Ignore. TODO: Return error
		return nil
	}
	// kubeadm join
	for i := 0; i < delta; i++ {
		if n.joinNextNode() == nil {
			n.currentSize++
		}
	}
	return nil
}
func (n *KubeadmNodeGroup) DecreaseTargetSize(delta int) error {
	klog.V(1).Infof("DBG: KubeadmNodeGroup.DecreaseTargetSize() CURRENT=%d DELTA=%d\n", n.currentSize, delta)
	if n.currentSize > delta {
		//TODO: kubeadm reset
		n.currentSize -= delta
		return nil
	}
        return fmt.Errorf("NODEGROUP.DecreaseTargetSize: Asked to delete %d nodes have %d nodes", delta, n.currentSize)
}
func (n *KubeadmNodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	instances := make([]cloudprovider.Instance, n.currentSize, n.maxSize)
	for i, nn := range n.nodes {
		if i >= n.currentSize {
			break
		}
		instance := cloudprovider.Instance{
			Id: nn.nodeName,
			Status: &cloudprovider.InstanceStatus{State: cloudprovider.InstanceRunning},
		}
		instances = append(instances, instance)
	}
	return instances, nil
}
func (n *KubeadmNodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	klog.V(1).Infof("DBG: KubeadmNodeGroup.DeleteNodes() NODES=%+v\n", nodes)
	if n.currentSize < len(nodes)  {
		return fmt.Errorf("NODEGROUP.DeleteNodes: Asked to delete %d nodes have %d nodes", len(nodes), n.currentSize)
	}
	//TODO: kubeadm reset for each node
	n.currentSize -= len(nodes)
	return nil
}
func (n *KubeadmNodeGroup) GetOptions(defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	klog.V(5).Info("DBG: KubeadmNodeGroup.GetOptions() DEFAULT_OPTIONS=%+v", defaults)
	return &defaults, nil
}
func (n *KubeadmNodeGroup) TemplateNodeInfo() (*schedulerframework.NodeInfo, error) {
	klog.V(5).Info("DBG: KubeadmNodeGroup.TemplateNodeInfo()")
	return n.nodeInfo, nil
}

type KubeadmCloudProvider struct {
	nodeGroups []cloudprovider.NodeGroup
}
func NewKubeadmCloudProvider(masterAddr, token, certHash string) cloudprovider.CloudProvider {
	ngs := make([]cloudprovider.NodeGroup, 0, 1)
	ng := NewKubeadmNodeGroup(masterAddr, token, certHash)
	ngs = append(ngs, ng)
	return &KubeadmCloudProvider{
	        nodeGroups: ngs,
	}
}
func (c *KubeadmCloudProvider) Name() string {
	klog.V(5).Info("DBG: KubeadmCloudProvider-Name()")
	return "KubeadmCloudProvider"
}
func (c *KubeadmCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	klog.V(5).Info("DBG: KubeadmCloudProvider-NodeGroups()")
	//TODO: Implement NodeGroups interface
	return c.nodeGroups
}
func (c *KubeadmCloudProvider) NodeGroupForNode(*apiv1.Node) (cloudprovider.NodeGroup, error) {
	klog.V(5).Info("DBG: KubeadmCloudProvider-NodeGroupForNode()")
	return c.nodeGroups[0], nil
}
func (c *KubeadmCloudProvider) HasInstance(*apiv1.Node) (bool, error) {
	klog.V(5).Info("DBG: KubeadmCloudProvider-HasInstance()")
	return false, fmt.Errorf("not implemented")
}
func (c *KubeadmCloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	klog.V(5).Info("DBG: KubeadmCloudProvider-Pricing()")
	return nil, nil
}
func (c *KubeadmCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	klog.V(5).Info("DBG: KubeadmCloudProvider-GetAvailableMachineTypes()")
	return nil, nil
}
func (c *KubeadmCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string,
                taints []apiv1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	klog.V(1).Infof("DBG: KubeadmCloudProvider-NewNodeGroup()")
	return nil, nil
}
func (c *KubeadmCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	klog.V(5).Info("DBG: KubeadmCloudProvider-GetResourceLimiter()")
	return nil, nil
}
func (c *KubeadmCloudProvider) GPULabel() string {
	klog.V(5).Info("DBG: KubeadmCloudProvider-GpuLabel()")
	return "KubeadmCloudProviderGPULabel"
}
func (c *KubeadmCloudProvider) GetAvailableGPUTypes() map[string]struct{} {
	klog.V(5).Info("DBG: KubeadmCloudProvider-GetAvailableGPUTypes()")
	return nil
}
func (c *KubeadmCloudProvider) GetNodeGpuConfig(*apiv1.Node) *cloudprovider.GpuConfig {
	klog.V(5).Info("DBG: KubeadmCloudProvider-GetNodeGpuConfig()")
	return nil
}
func (c *KubeadmCloudProvider) Cleanup() error {
	klog.V(5).Info("DBG: KubeadmCloudProvider-Cleanup()\n")
	return nil
}
func (c *KubeadmCloudProvider) Refresh() error {
	klog.V(5).Info("DBG: KubeadmCloudProvider-Refresh()\n")
	return nil
}

type KubeadmCloudProviderServer struct {
	protos.UnimplementedCloudProviderServer
	provider cloudprovider.CloudProvider
}
func NewKubeadmCloudProviderServer(cp cloudprovider.CloudProvider) protos.CloudProviderServer {
	return &KubeadmCloudProviderServer{
		provider: cp,
	}
}
func (s *KubeadmCloudProviderServer) getNodeGroup(id string) cloudprovider.NodeGroup {
        for _, n := range s.provider.NodeGroups() {
                if n.Id() == id {
                        return n
                }
        }
        return nil
}
func apiv1Node(pbNode *protos.ExternalGrpcNode) *apiv1.Node {
        apiv1Node := &apiv1.Node{}
        apiv1Node.ObjectMeta = metav1.ObjectMeta{
                Name:        pbNode.GetName(),
                Annotations: pbNode.GetAnnotations(),
                Labels:      pbNode.GetLabels(),
        }
        apiv1Node.Spec = apiv1.NodeSpec{
                ProviderID: pbNode.GetProviderID(),
        }
        return apiv1Node
}
func pbNodeGroup(ng cloudprovider.NodeGroup) *protos.NodeGroup {
        return &protos.NodeGroup{
                Id:      ng.Id(),
                MaxSize: int32(ng.MaxSize()),
                MinSize: int32(ng.MinSize()),
                Debug:   ng.Debug(),
        }
}
func (s *KubeadmCloudProviderServer) NodeGroups(_ context.Context, req *protos.NodeGroupsRequest) (*protos.NodeGroupsResponse, error) {
	klog.V(5).Info("DBG: KCPServer-NodeGroups()")
        debug(req)
        pbNgs := make([]*protos.NodeGroup, 0)
        for _, ng := range s.provider.NodeGroups() {
                pbNgs = append(pbNgs, pbNodeGroup(ng))
        }
        return &protos.NodeGroupsResponse{
                NodeGroups: pbNgs,
        }, nil
}
func (s *KubeadmCloudProviderServer) NodeGroupNodes(_ context.Context, req *protos.NodeGroupNodesRequest) (*protos.NodeGroupNodesResponse, error) {
	klog.V(5).Info("DBG: KCPServer-NodeGroupNodes()")
        debug(req)
        id := req.GetId()
        ng := s.getNodeGroup(id)
        if ng == nil {
                return nil, fmt.Errorf("NodeGroup %q, not found", id)
        }
        instances, err := ng.Nodes()
        if err != nil {
                return nil, err
        }
        pbInstances := make([]*protos.Instance, 0)
        for _, i := range instances {
                pbInstance := new(protos.Instance)
                pbInstance.Id = i.Id
                if i.Status == nil {
                        pbInstance.Status = &protos.InstanceStatus{
                                InstanceState: protos.InstanceStatus_unspecified,
                                ErrorInfo:     &protos.InstanceErrorInfo{},
                        }
                } else {
                        pbInstance.Status = new(protos.InstanceStatus)
                        pbInstance.Status.InstanceState = protos.InstanceStatus_InstanceState(i.Status.State)
                        if i.Status.ErrorInfo == nil {
                                pbInstance.Status.ErrorInfo = &protos.InstanceErrorInfo{}
                        } else {
                                pbInstance.Status.ErrorInfo = &protos.InstanceErrorInfo{
                                        ErrorCode:          i.Status.ErrorInfo.ErrorCode,
                                        ErrorMessage:       i.Status.ErrorInfo.ErrorMessage,
                                        InstanceErrorClass: int32(i.Status.ErrorInfo.ErrorClass),
                                }
                        }
                }
                pbInstances = append(pbInstances, pbInstance)
        }
        return &protos.NodeGroupNodesResponse{
                Instances: pbInstances,
        }, nil
}
func (s *KubeadmCloudProviderServer) NodeGroupForNode(_ context.Context, req *protos.NodeGroupForNodeRequest) (*protos.NodeGroupForNodeResponse, error) {
	klog.V(5).Info("DBG: KCPServer-NodeGroupForNode()")
        debug(req)
        pbNode := req.GetNode()
        if pbNode == nil {
                return nil, fmt.Errorf("request fields were nil")
        }
        node := apiv1Node(pbNode)
        ng, err := s.provider.NodeGroupForNode(node)
        if err != nil {
                return nil, err
        }
        if ng == nil {
                return &protos.NodeGroupForNodeResponse{
                        NodeGroup: &protos.NodeGroup{}, //NodeGroup with id = "", meaning the node should not be processed by cluster autoscaler
                }, nil
        }
        return &protos.NodeGroupForNodeResponse{
                NodeGroup: pbNodeGroup(ng),
        }, nil
}
func (s *KubeadmCloudProviderServer) NodeGroupGetOptions(_ context.Context, req *protos.NodeGroupAutoscalingOptionsRequest) (*protos.NodeGroupAutoscalingOptionsResponse, error) {
	klog.V(5).Info("DBG: KCPServer-NodeGroupGetOptions()")
        debug(req)
        id := req.GetId()
        ng := s.getNodeGroup(id)
        if ng == nil {
                return nil, fmt.Errorf("NodeGroup %q, not found", id)
        }
        pbDefaults := req.GetDefaults()
        if pbDefaults == nil {
                return nil, fmt.Errorf("request fields were nil")
        }
        defaults := config.NodeGroupAutoscalingOptions{
                ScaleDownUtilizationThreshold:    pbDefaults.GetScaleDownGpuUtilizationThreshold(),
                ScaleDownGpuUtilizationThreshold: pbDefaults.GetScaleDownGpuUtilizationThreshold(),
                ScaleDownUnneededTime:            pbDefaults.GetScaleDownUnneededTime().Duration,
                ScaleDownUnreadyTime:             pbDefaults.GetScaleDownUnneededTime().Duration,
                MaxNodeProvisionTime:             pbDefaults.GetMaxNodeProvisionTime().Duration,
        }
        opts, err := ng.GetOptions(defaults)
        if err != nil {
                if err == cloudprovider.ErrNotImplemented {
                        return nil, status.Error(codes.Unimplemented, err.Error())
                }
                return nil, err
        }
        if opts == nil {
                return nil, fmt.Errorf("GetOptions not implemented") //make this explicitly so that grpc response is discarded
        }
        return &protos.NodeGroupAutoscalingOptionsResponse{
                NodeGroupAutoscalingOptions: &protos.NodeGroupAutoscalingOptions{
                        ScaleDownUtilizationThreshold:    opts.ScaleDownUtilizationThreshold,
                        ScaleDownGpuUtilizationThreshold: opts.ScaleDownGpuUtilizationThreshold,
                        ScaleDownUnneededTime: &metav1.Duration{
                                Duration: opts.ScaleDownUnneededTime,
                        },
                        ScaleDownUnreadyTime: &metav1.Duration{
                                Duration: opts.ScaleDownUnreadyTime,
                        },
                        MaxNodeProvisionTime: &metav1.Duration{
                                Duration: opts.MaxNodeProvisionTime,
                        },
                },
        }, nil
}
func (s *KubeadmCloudProviderServer) NodeGroupTemplateNodeInfo(_ context.Context, req *protos.NodeGroupTemplateNodeInfoRequest) (*protos.NodeGroupTemplateNodeInfoResponse, error) {
	klog.V(5).Info("DBG: KCPServer-NodeGroupTemplateNodeInfo()")
        debug(req)
        id := req.GetId()
        ng := s.getNodeGroup(id)
        if ng == nil {
                return nil, fmt.Errorf("NodeGroup %q, not found", id)
        }
        info, err := ng.TemplateNodeInfo()
        if err != nil {
                if err == cloudprovider.ErrNotImplemented {
                        return nil, status.Error(codes.Unimplemented, err.Error())
                }
                return nil, err
        }
        return &protos.NodeGroupTemplateNodeInfoResponse{
                NodeInfo: info.Node(),
        }, nil
}
func (s *KubeadmCloudProviderServer) NodeGroupTargetSize(_ context.Context, req *protos.NodeGroupTargetSizeRequest) (*protos.NodeGroupTargetSizeResponse, error) {
	klog.V(5).Info("DBG: KCPServer-NodeGroupTargetSize()")
        debug(req)
        id := req.GetId()
        ng := s.getNodeGroup(id)
        if ng == nil {
                return nil, fmt.Errorf("NodeGroup %q, not found", id)
        }
        size, err := ng.TargetSize()
        if err != nil {
                return nil, err
        }
        return &protos.NodeGroupTargetSizeResponse{
                TargetSize: int32(size),
        }, nil
}
func (s *KubeadmCloudProviderServer) NodeGroupIncreaseSize(_ context.Context, req *protos.NodeGroupIncreaseSizeRequest) (*protos.NodeGroupIncreaseSizeResponse, error) {
	klog.V(5).Info("DBG: KCPServer-NodeGroupIncreaseSize()")
        debug(req)
        id := req.GetId()
        ng := s.getNodeGroup(id)
        if ng == nil {
                return nil, fmt.Errorf("NodeGroup %q, not found", id)
        }
        err := ng.IncreaseSize(int(req.GetDelta()))
        if err != nil {
                return nil, err
        }
        return &protos.NodeGroupIncreaseSizeResponse{}, nil
}
func (s *KubeadmCloudProviderServer) NodeGroupDecreaseTargetSize(_ context.Context, req *protos.NodeGroupDecreaseTargetSizeRequest) (*protos.NodeGroupDecreaseTargetSizeResponse, error) {
	klog.V(5).Info("DBG: KCPServer-NodeGroupDecreaseTargetSize()")
        debug(req)
        id := req.GetId()
        ng := s.getNodeGroup(id)
        if ng == nil {
                return nil, fmt.Errorf("NodeGroup %q, not found", id)
        }
        err := ng.DecreaseTargetSize(int(req.GetDelta()))
        if err != nil {
                return nil, err
        }
        return &protos.NodeGroupDecreaseTargetSizeResponse{}, nil
}
func (s *KubeadmCloudProviderServer) NodeGroupDeleteNodes(_ context.Context, req *protos.NodeGroupDeleteNodesRequest) (*protos.NodeGroupDeleteNodesResponse, error) {
	klog.V(5).Info("DBG: KCPServer-NodeGroupDeleteNodes()")
        debug(req)
        id := req.GetId()
        ng := s.getNodeGroup(id)
        if ng == nil {
                return nil, fmt.Errorf("NodeGroup %q, not found", id)
        }
        nodes := make([]*apiv1.Node, 0)
        for _, n := range req.GetNodes() {
                nodes = append(nodes, apiv1Node(n))
        }
        err := ng.DeleteNodes(nodes)
        if err != nil {
                return nil, err
        }
        return &protos.NodeGroupDeleteNodesResponse{}, nil
}
func (s *KubeadmCloudProviderServer) PricingNodePrice(_ context.Context, req *protos.PricingNodePriceRequest) (*protos.PricingNodePriceResponse, error) {
	klog.V(5).Info("DBG: KCPServer-PricingNodePrice()")
        debug(req)
        model, err := s.provider.Pricing()
        if err != nil {
                if err == cloudprovider.ErrNotImplemented {
                        return nil, status.Error(codes.Unimplemented, err.Error())
                }
                return nil, err
        }
        reqNode := req.GetNode()
        reqStartTime := req.GetStartTime()
        reqEndTime := req.GetEndTime()
        if reqNode == nil || reqStartTime == nil || reqEndTime == nil {
                return nil, fmt.Errorf("request fields were nil")
        }
        price, nodePriceErr := model.NodePrice(apiv1Node(reqNode), reqStartTime.Time, reqEndTime.Time)
        if nodePriceErr != nil {
                return nil, nodePriceErr
        }
        return &protos.PricingNodePriceResponse{
                Price: price,
        }, nil
}
func (s *KubeadmCloudProviderServer) PricingPodPrice(_ context.Context, req *protos.PricingPodPriceRequest) (*protos.PricingPodPriceResponse, error) {
	klog.V(5).Info("DBG: KCPServer-PricingPodPrice()")
        debug(req)
        model, err := s.provider.Pricing()
        if err != nil {
                if err == cloudprovider.ErrNotImplemented {
                        return nil, status.Error(codes.Unimplemented, err.Error())
                }
                return nil, err
        }
        reqPod := req.GetPod()
        reqStartTime := req.GetStartTime()
        reqEndTime := req.GetEndTime()
        if reqPod == nil || reqStartTime == nil || reqEndTime == nil {
                return nil, fmt.Errorf("request fields were nil")
        }
        price, podPriceErr := model.PodPrice(reqPod, reqStartTime.Time, reqEndTime.Time)
        if podPriceErr != nil {
                return nil, podPriceErr
        }
        return &protos.PricingPodPriceResponse{
                Price: price,
        }, nil
}
func (s *KubeadmCloudProviderServer) GPULabel(_ context.Context, req *protos.GPULabelRequest) (*protos.GPULabelResponse, error) {
	klog.V(5).Info("DBG: KCPServer-GPULabel()")
        debug(req)
        label := s.provider.GPULabel()
        return &protos.GPULabelResponse{
                Label: label,
        }, nil
}
func (s *KubeadmCloudProviderServer) GetAvailableGPUTypes(_ context.Context, req *protos.GetAvailableGPUTypesRequest) (*protos.GetAvailableGPUTypesResponse, error) {
	klog.V(5).Info("DBG: KCPServer-GetAvailableGPUTypes()")
        debug(req)
        types := s.provider.GetAvailableGPUTypes()
        pbGpuTypes := make(map[string]*anypb.Any)
        for t := range types {
                pbGpuTypes[t] = nil
        }
        return &protos.GetAvailableGPUTypesResponse{
                GpuTypes: pbGpuTypes,
        }, nil
}
func (s *KubeadmCloudProviderServer) Refresh(_ context.Context, req *protos.RefreshRequest) (*protos.RefreshResponse, error) {
	klog.V(5).Info("DBG: KCPServer-Refresh()")
        debug(req)
        err := s.provider.Refresh()
        return &protos.RefreshResponse{}, err
}
func (s *KubeadmCloudProviderServer) Cleanup(_ context.Context, req *protos.CleanupRequest) (*protos.CleanupResponse, error) {
	klog.V(5).Info("DBG: KCPServer-Cleanup()")
	debug(req);
	return &protos.CleanupResponse{}, nil
}

func main() {
	var grpcSvr *grpc.Server

	klog.InitFlags(nil)
	kube_flag.InitFlags()
	grpcSvr = grpc.NewServer()

	klog.V(1).Infof("DBG: API-Address: '%s' Token: '%s' Cert-Hash: '%s'\n", *apiAddress, *token, *certHash)

	kubeadmCloudProvider := NewKubeadmCloudProvider(*apiAddress, *token, *certHash)
	klog.V(5).Info("DBG: kubeadmCloudProvider: '%+v'\n", kubeadmCloudProvider)

	kubeadmCloudProviderSvr := NewKubeadmCloudProviderServer(kubeadmCloudProvider)
	klog.V(5).Info("DBG: kubeadmCloudProviderSvr: '%+v'\n", kubeadmCloudProviderSvr)

	klog.V(1).Infof("DBG: Listening on address: %s\n", *address)
	// listen
	lis, err := net.Listen("tcp", *address)
	if err != nil {
		klog.Fatalf("failed to listen: %s", err)
	}
	klog.V(5).Info("DBG: grpcSvr: %p, lis: %p\n", grpcSvr, lis)

	// serve
	protos.RegisterCloudProviderServer(grpcSvr, kubeadmCloudProviderSvr)
	klog.V(1).Infof("DBG: Server ready at: %s\n", *address)
	if err := grpcSvr.Serve(lis); err != nil {
		klog.Fatalf("failed to serve: %v", err)
	}
}
