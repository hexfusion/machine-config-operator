package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog"

	"github.com/golang/glog"
	v1 "github.com/openshift/api/config/v1"
	operatorclient "github.com/openshift/client-go/operator/clientset/versioned/typed/operator/v1"
	"github.com/openshift/machine-config-operator/pkg/version"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	EtcdScalingAnnotationKey = "etcd.operator.openshift.io/scale"
	assetDir                 = "/etc/ssl/etcd"
)

var (
	runCmd = &cobra.Command{
		Use:   "run",
		Short: "Runs the setup-etcd-environment",
		Long:  "",
		RunE:  runRunCmd,
	}

	runOpts struct {
		discoverySRV string
		ifName       string
		outputFile   string
		bootstrapSRV bool
	}
)

//TODO short term hack until we merge CEO upstream

type EtcdScaling struct {
	Metadata *metav1.ObjectMeta `json:"metadata,omitempty"`
	Members  []Member           `json:"members,omitempty"`
	PodFQDN  string             `json:"podFQDN,omitempty"`
}

type Member struct {
	ID         uint64            `json:"ID,omitempty"`
	Name       string            `json:"name,omitempty"`
	PeerURLS   []string          `json:"peerURLs,omitempty"`
	ClientURLS []string          `json:"clientURLs,omitempty"`
	Conditions []MemberCondition `json:"conditions,omitempty"`
}

type MemberCondition struct {
	// type describes the current condition
	Type MemberConditionType `json:"type"`
	// status is the status of the condition (True, False, Unknown)
	Status v1.ConditionStatus `json:"status"`
	// timestamp for the last update to this condition
	// +optional
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// reason is the reason for the condition's last transition.
	// +optional
	Reason string `json:"reason,omitempty"`
	// message is a human-readable explanation containing details about
	// the transition
	// +optional
	Message string `json:"message,omitempty"`
}

type MemberConditionType string

const (
	// Ready indicated the member is part of the cluster and available
	MemberReady MemberConditionType = "Ready"
	// Unknown indicated the member is part of the cluster but condition is unknown
	MemberUnknown MemberConditionType = "Unknown"
	// Degraded indicates the member pod is in a degraded state and should be restarted
	MemberDegraded MemberConditionType = "Degraded"
	// Remove indicates the member should be removed from the cluster
	MemberRemove MemberConditionType = "Remove"
	// MemberAdd is a member who is ready to join cluster but currently has not.
	MemberAdd MemberConditionType = "Add"
)

func GetMemberCondition(status string) MemberConditionType {
	switch {
	case status == string(MemberReady):
		return MemberReady
	case status == string(MemberRemove):
		return MemberRemove
	case status == string(MemberUnknown):
		return MemberUnknown
	case status == string(MemberDegraded):
		return MemberDegraded
	case status == string(MemberAdd):
		return MemberAdd
	}

	return ""
}

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.PersistentFlags().StringVar(&runOpts.discoverySRV, "discovery-srv", "", "DNS domain used to populate envs from SRV query.")
	rootCmd.PersistentFlags().StringVar(&runOpts.outputFile, "output-file", "", "file where the envs are written. If empty, prints to Stdout.")
	rootCmd.PersistentFlags().BoolVar(&runOpts.bootstrapSRV, "bootstrap-srv", true, "use SRV discovery for bootstraping etcd cluster.")
}

func runRunCmd(cmd *cobra.Command, args []string) error {
	flag.Set("logtostderr", "true")
	flag.Parse()

	// To help debugging, immediately log version
	glog.Infof("Version: %+v (%s)", version.Raw, version.Hash)

	if runOpts.discoverySRV == "" {
		return errors.New("--discovery-srv cannot be empty")
	}

	etcdName := os.Getenv("ETCD_NAME")
	if etcdName == "" {
		return fmt.Errorf("environment variable ETCD_NAME has no value")
	}

	etcdDataDir := os.Getenv("ETCD_DATA_DIR")
	if etcdDataDir == "" {
		return fmt.Errorf("environment variable ETCD_DATA_DIR has no value")
	}

	if !inCluster() {
		glog.V(4).Infof("KUBERNETES_SERVICE_HOST or KUBERNETES_SERVICE_PORT contain no value, running in standalone mode.")
	}

	ips, err := ipAddrs()
	if err != nil {
		return err
	}

	var dns string
	var ip string
	if err := wait.PollImmediate(30*time.Second, 5*time.Minute, func() (bool, error) {
		for _, cand := range ips {
			found, err := reverseLookup("etcd-server-ssl", "tcp", runOpts.discoverySRV, cand, runOpts.bootstrapSRV)
			if err != nil {
				glog.Errorf("error looking up self: %v", err)
				continue
			}
			if found != "" {
				dns = found
				ip = cand
				return true, nil
			}
			glog.V(4).Infof("no matching dns for %s in %s: %v", cand, runOpts.discoverySRV, err)
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("could not find self: %v", err)
	}
	glog.Infof("dns name is %s", dns)

	exportEnv := make(map[string]string)
	if _, err := os.Stat(fmt.Sprintf("%s/member", etcdDataDir)); os.IsNotExist(err) && !runOpts.bootstrapSRV && inCluster() {
		duration := 10 * time.Second
		// wait for our SA asssets to sync
		wait.PollInfinite(duration, func() (bool, error) {
			if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); os.IsNotExist(err) {
				glog.Errorf("serviceaccount failed: %v", err)
				return false, nil
			}
			return true, nil
		})

		clientConfig, err := rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}
		operatorClient, err := operatorclient.NewForConfig(clientConfig)
		if err != nil {
			return err
		}
		client, err := kubernetes.NewForConfig(clientConfig)
		if err != nil {
			return fmt.Errorf("error creating client: %v", err)
		}
		etcdClient := operatorClient.Etcds()

		// this handles condition where we are in the proccess of or are post removal but waiting for MemberAdd status to scaleup.
		wait.PollInfinite(duration, func() (bool, error) {
			if isMemberAdd(etcdClient, etcdName) {
				return true, nil
			}
			glog.V(4).Infof("waiting for MemberAdd status")
			return false, nil
		})

		var e EtcdScaling
		// wait forever for success and retry every duration interval
		wait.PollInfinite(duration, func() (bool, error) {
			result, err := client.CoreV1().ConfigMaps("openshift-etcd").Get("member-config", metav1.GetOptions{})
			if err != nil {
				glog.Errorf("error creating client %v", err)
				return false, nil
			}
			if err := json.Unmarshal([]byte(result.Annotations[EtcdScalingAnnotationKey]), &e); err != nil {
				glog.Errorf("error decoding result %v", err)
				return false, nil
			}
			if e.Metadata.Name != etcdName {
				glog.Errorf("could not find self in member-config")
				return false, nil
			}
			members := e.Members
			if len(members) == 0 {
				glog.Errorf("no members found in member-config")
				return false, nil
			}
			var memberList []string
			for _, m := range members {
				memberList = append(memberList, fmt.Sprintf("%s=%s", m.Name, m.PeerURLS[0]))
			}
			memberList = append(memberList, fmt.Sprintf("%s=https://%s:2380", etcdName, dns))
			exportEnv["INITIAL_CLUSTER"] = strings.Join(memberList, ",")
			exportEnv["INITIAL_CLUSTER_STATE"] = "existing"
			return true, nil
		})
	}

	out := os.Stdout
	if runOpts.outputFile != "" {
		f, err := os.Create(runOpts.outputFile)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}

	if runOpts.bootstrapSRV {
		exportEnv["DISCOVERY_SRV"] = runOpts.discoverySRV
	} else {
		exportEnv["NAME"] = etcdName
	}

	// enable etcd to run using s390 and s390x. Because these are not officially supported upstream
	// etcd requires population of environment variable ETCD_UNSUPPORTED_ARCH at runtime.
	// https://github.com/etcd-io/etcd/blob/master/Documentation/op-guide/supported-platform.md
	arch := runtime.GOARCH
	if strings.HasPrefix(arch, "s390") {
		exportEnv["UNSUPPORTED_ARCH"] = arch
	}
	if err := writeEnvironmentFile(exportEnv, out, true); err != nil {
		return err
	}

	return writeEnvironmentFile(map[string]string{
		"IPV4_ADDRESS":      ip,
		"DNS_NAME":          dns,
		"WILDCARD_DNS_NAME": fmt.Sprintf("*.%s", runOpts.discoverySRV),
	}, out, false)
}

func ipAddrs() ([]string, error) {
	var ips []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ips, err
	}
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil {
			continue
		}
		ip = ip.To4()
		if ip == nil {
			continue // not an ipv4 address
		}
		if !ip.IsGlobalUnicast() {
			continue // we only want global unicast address
		}
		ips = append(ips, ip.String())
	}

	return ips, nil
}

func reverseLookup(service, proto, name, self string, bootstrapSRV bool) (string, error) {
	if bootstrapSRV || inCluster() {
		return reverseLookupSelf(service, proto, name, self)
	}
	return lookupTargetMatchSelf(fmt.Sprintf("etcd-bootstrap.%s", name), self)
}

// returns the target from the SRV record that resolves to self.
func reverseLookupSelf(service, proto, name, self string) (string, error) {
	_, srvs, err := net.LookupSRV(service, proto, name)
	if err != nil {
		return "", err
	}
	for _, srv := range srvs {
		glog.V(4).Infof("checking against %s", srv.Target)
		selfTarget, err := lookupTargetMatchSelf(srv.Target, self)
		if err != nil {
			return "", err
		}
		if selfTarget != "" {
			return selfTarget, nil
		}
	}
	return "", fmt.Errorf("could not find self")
}

//
func lookupTargetMatchSelf(target string, self string) (string, error) {
	addrs, err := net.LookupHost(target)
	if err != nil {
		return "", fmt.Errorf("could not resolve member %q", target)
	}
	selfTarget := ""
	for _, addr := range addrs {
		if addr == self {
			selfTarget = strings.Trim(target, ".")
			break
		}
	}
	return selfTarget, nil
}

func writeEnvironmentFile(m map[string]string, w io.Writer, export bool) error {
	var buffer bytes.Buffer
	for k, v := range m {
		env := fmt.Sprintf("ETCD_%s=%s\n", k, v)
		if export == true {
			env = fmt.Sprintf("export %s", env)
		}
		buffer.WriteString(env)
	}
	if _, err := buffer.WriteTo(w); err != nil {
		return err
	}
	return nil
}

func inCluster() bool {
	if os.Getenv("KUBERNETES_SERVICE_HOST") == "" || os.Getenv("KUBERNETES_SERVICE_PORT") == "" {
		return false
	}
	return true
}

//TODO util me
func isMemberAdd(client operatorclient.EtcdInterface, name string) bool {
	members, err := pendingMemberList(client)
	if err != nil {
		klog.Errorf("isMemberAdd: error %v", err)
	}
	for _, m := range members {
		if m.Name == name && m.Conditions[0].Type == MemberAdd {
			return true
		}
	}
	return false
}

//TODO util me
func pendingMemberList(client operatorclient.EtcdInterface) ([]Member, error) {
	configPath := []string{"cluster", "pending"}
	operatorSpec, err := client.Get("cluster", metav1.GetOptions{})
	if err != nil {
		glog.V(4).Infof("pendingMemberList: error %v", err)
		return nil, err
	}

	config := map[string]interface{}{}
	if err := json.NewDecoder(bytes.NewBuffer(operatorSpec.Spec.ObservedConfig.Raw)).Decode(&config); err != nil {
		klog.V(4).Infof("decode of existing config failed with error: %v", err)
	}
	data, exists, err := unstructured.NestedSlice(config, configPath...)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("etcd cluster members not observed")
	}

	// populate pending  members as observed.
	var members []Member
	for _, member := range data {
		memberMap, _ := member.(map[string]interface{})
		name, exists, err := unstructured.NestedString(memberMap, "name")
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("member name does not exist")
		}
		peerURLs, exists, err := unstructured.NestedString(memberMap, "peerURLs")
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("member peerURLs do not exist")
		}
		status, exists, err := unstructured.NestedString(memberMap, "status")
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("member status does not exist")
		}

		condition := GetMemberCondition(status)
		m := Member{
			Name:     name,
			PeerURLS: []string{peerURLs},
			Conditions: []MemberCondition{
				{
					Type: condition,
				},
			},
		}
		members = append(members, m)
	}
	return members, nil
}
