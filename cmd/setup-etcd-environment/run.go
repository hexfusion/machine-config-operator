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

	"github.com/golang/glog"
	"github.com/openshift/machine-config-operator/pkg/version"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const EtcdScalingAnnotationKey = "etcd.operator.openshift.io/scale"

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
		standalone   bool
	}
)

type EtcdScaling struct {
	Metadata *metav1.ObjectMeta `json:"metadata,omitempty"`
	Members  []Member           `json:"members,omitempty"`
}

type Member struct {
	ID         uint64   `json:"ID,omitempty"`
	Name       string   `json:"name,omitempty"`
	PeerURLS   []string `json:"peerURLs,omitempty"`
	ClientURLS []string `json:"clientURLs,omitempty"`
}

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.PersistentFlags().StringVar(&runOpts.discoverySRV, "discovery-srv", "", "DNS domain used to bootstrap initial etcd cluster.")
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

	if os.Getenv("KUBERNETES_SERVICE_HOST") == "" || os.Getenv("KUBERNETES_SERVICE_PORT") == "" {
		glog.V(4).Infof("KUBERNETES_SERVICE_HOST or KUBERNETES_SERVICE_PORT contain no value enabling standalone mode")
		runOpts.standalone = true
	}

	ips, err := ipAddrs()
	if err != nil {
		return err
	}

	var dns string
	var ip string
	if err := wait.PollImmediate(30*time.Second, 5*time.Minute, func() (bool, error) {
		for _, cand := range ips {
			found, err := reverseLookup("etcd-server-ssl", "tcp", runOpts.discoverySRV, cand, runOpts.bootstrapSRV, runOpts.standalone)
			if err != nil {
				glog.Errorf("error looking up self: %v", err)
				continue
			}
			if found != "" {
				dns = found
				ip = cand
				return true, nil
			}
			glog.V(4).Infof("no matching dns for %s", cand)
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("could not find self: %v", err)
	}
	glog.Infof("dns name is %s", dns)

	exportEnv := make(map[string]string)
	if _, err := os.Stat(fmt.Sprintf("%s/member", etcdDataDir)); os.IsNotExist(err) && !runOpts.bootstrapSRV && !runOpts.standalone {
		clientConfig, err := rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}
		client, err := kubernetes.NewForConfig(clientConfig)
		if err != nil {
			return fmt.Errorf("error creating client: %v", err)
		}
		var e EtcdScaling
		duration := 10 * time.Second
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

func reverseLookup(service, proto, name, self string, bootstrapSRV bool, standalone bool) (string, error) {
	if bootstrapSRV || !standalone {
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
