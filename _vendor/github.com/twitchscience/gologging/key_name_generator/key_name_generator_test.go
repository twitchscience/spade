package key_name_generator

import (
	"strings"
	"testing"
)

func TestEdgeKeyNameGenerator(t *testing.T) {
	gen := &EdgeKeyNameGenerator{
		Info: &InstanceInfo{
			Service:        "test",
			Cluster:        "testCluster",
			AutoScaleGroup: "testCluster",
			Node:           "testNode",
			LoggingDir:     "",
		},
	}
	test1 := gen.GetKeyName("blah")
	if !strings.Contains(test1, "/testCluster/") ||
		!strings.Contains(test1, ".testNode.") {
		t.Errorf("expected %s but got %s\n",
			"%s/testCluster/%d.testNode.%s.log.gz",
			test1,
		)
	}
}

func TestProcessorKeyNameGenerator(t *testing.T) {
	gen := &ProcessorKeyNameGenerator{
		Info: &InstanceInfo{
			Service:        "test",
			Cluster:        "testCluster",
			AutoScaleGroup: "testCluster",
			Node:           "testNode",
			LoggingDir:     "",
		},
	}
	test1 := gen.GetKeyName("blah.v0")
	start := strings.Index(test1, "/")
	if !strings.Contains(test1[start:], "blah/v0/testCluster/testNode.") {
		t.Errorf("expected %s but got %s\n",
			"blah/v0/testCluster/testNode.",
			test1,
		)
	}

	test2 := gen.GetKeyName("/extra/blah.v0")
	start2 := strings.Index(test2, "/")
	if !strings.Contains(test2[start2:], "blah/v0/testCluster/testNode.") {
		t.Errorf("expected %s but got %s\n",
			"blah/v0/testCluster/testNode.",
			test2,
		)
	}

	test3 := gen.GetKeyName("/extra/blah.v0.gz")
	start3 := strings.Index(test3, "/")
	if !strings.Contains(test3[start3:], "blah/v0/testCluster/testNode.") {
		t.Errorf("expected %s but got %s\n",
			"blah/v0/testCluster/testNode.",
			test3,
		)
	}

	test4 := gen.GetKeyName("/extra/blah.v0.gz")
	start4 := strings.Index(test4, "/")
	if !strings.Contains(test4[start4:], "blah/v0/testCluster/testNode.") {
		t.Errorf("expected %s but got %s\n",
			"blah/v0/testCluster/testNode.",
			test4,
		)
	}
}
