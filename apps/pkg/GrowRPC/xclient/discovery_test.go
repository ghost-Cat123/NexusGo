package xclient

import "testing"

func TestMultiServersDiscoveryConsistentHashUsesStableUserKey(t *testing.T) {
	discovery := NewMultiServerDiscovery([]string{
		"tcp@127.0.0.1:8001",
		"tcp@127.0.0.1:8002",
		"tcp@127.0.0.1:8003",
	})

	first, err := discovery.Get(ConsistentHash, "10001")
	if err != nil {
		t.Fatalf("select first instance: %v", err)
	}
	for i := 0; i < 20; i++ {
		got, err := discovery.Get(ConsistentHash, "10001")
		if err != nil {
			t.Fatalf("select instance: %v", err)
		}
		if got != first {
			t.Fatalf("same user key selected different instances: first=%s got=%s", first, got)
		}
	}
}

func TestMultiServersDiscoveryConsistentHashRequiresKey(t *testing.T) {
	discovery := NewMultiServerDiscovery([]string{"tcp@127.0.0.1:8001"})
	if _, err := discovery.Get(ConsistentHash, ""); err == nil {
		t.Fatal("expected an error for an empty consistent-hash key")
	}
}
