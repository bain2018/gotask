package mongo_client

import (
	"os"
	"strings"
	"testing"
)

func TestAppTemplatesUseConfiguredMinPoolSize(t *testing.T) {
	for _, path := range []string{"../../cmd/app.go", "../../publish/app.go"} {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}

		source := string(content)
		if !strings.Contains(source, "SetMinPoolSize(mongoConfig.MinPoolSize)") {
			t.Fatalf("%s does not use configured MinPoolSize", path)
		}
		if strings.Contains(source, "SetMinPoolSize(mongoConfig.MaxPoolSize)") {
			t.Fatalf("%s still uses MaxPoolSize as MinPoolSize", path)
		}
	}
}
