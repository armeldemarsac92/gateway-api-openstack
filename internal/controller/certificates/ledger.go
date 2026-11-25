package certificates

import (
	"encoding/json/v2"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const maxSnapshotBytes = 1 << 20

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

type bindingScope struct {
	Cluster      string `json:"cluster"`
	GatewayClass string `json:"gatewayClass"`
	Project      string `json:"project"`
	Region       string `json:"region"`
}

func (s bindingScope) valid() bool {
	return s.Cluster != "" && s.Cluster == strings.TrimSpace(s.Cluster) &&
		s.Project != "" && s.Project == strings.TrimSpace(s.Project) &&
		s.Region != "" && s.Region == strings.TrimSpace(s.Region) &&
		len(validation.IsDNS1123Subdomain(s.GatewayClass)) == 0
}

type binding struct {
	Namespace        string    `json:"namespace"`
	Name             string    `json:"name"`
	UID              string    `json:"uid"`
	ResourceVersion  string    `json:"resourceVersion"`
	BarbicanSecretID string    `json:"barbicanSecretID"`
	ChainSHA256      []string  `json:"chainSHA256"`
	NotBefore        time.Time `json:"notBefore"`
	NotAfter         time.Time `json:"notAfter"`
}

func (b binding) valid() bool {
	identifier, err := uuid.Parse(b.BarbicanSecretID)
	if err != nil || identifier.String() != b.BarbicanSecretID ||
		identifier.Variant() != uuid.RFC4122 || identifier.Version() != 4 ||
		b.UID == "" || b.ResourceVersion == "" || len(b.ChainSHA256) == 0 ||
		b.NotBefore.IsZero() || !b.NotBefore.Before(b.NotAfter) {
		return false
	}
	seen := make(map[string]struct{}, len(b.ChainSHA256))
	for _, fingerprint := range b.ChainSHA256 {
		if _, duplicate := seen[fingerprint]; duplicate || !sha256Hex.MatchString(fingerprint) {
			return false
		}
		seen[fingerprint] = struct{}{}
	}
	return true
}

func (b binding) validAt(now time.Time) bool {
	return !now.Before(b.NotBefore) && now.Before(b.NotAfter)
}

func validName(target client.ObjectKey) bool {
	return len(validation.IsDNS1123Label(target.Namespace)) == 0 &&
		len(validation.IsDNS1123Subdomain(target.Name)) == 0
}

func findBinding(path, purpose string, scope bindingScope, target client.ObjectKey) (binding, bool, error) {
	if !scope.valid() {
		return binding{}, false, nil
	}
	var snapshot struct {
		Version   int          `json:"version"`
		Purpose   string       `json:"purpose"`
		Scope     bindingScope `json:"scope"`
		Revisions []binding    `json:"revisions"`
	}
	info, err := os.Stat(path)
	if err != nil {
		return binding{}, false, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxSnapshotBytes {
		return binding{}, false, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return binding{}, false, err
	}
	defer func() { _ = file.Close() }()
	payload, err := io.ReadAll(io.LimitReader(file, maxSnapshotBytes+1))
	if err != nil {
		return binding{}, false, err
	}
	if len(payload) > maxSnapshotBytes ||
		json.Unmarshal(payload, &snapshot, json.RejectUnknownMembers(true)) != nil ||
		snapshot.Version != 1 || snapshot.Purpose != purpose || snapshot.Scope != scope || snapshot.Revisions == nil {
		return binding{}, false, nil
	}
	var selected binding
	seen := make(map[client.ObjectKey]struct{}, len(snapshot.Revisions))
	for _, row := range snapshot.Revisions {
		key := client.ObjectKey{Namespace: row.Namespace, Name: row.Name}
		if _, duplicate := seen[key]; duplicate || !validName(key) || !row.valid() {
			return binding{}, false, nil
		}
		seen[key] = struct{}{}
		if key == target {
			selected = row
		}
	}
	return selected, selected.Name != "", nil
}
