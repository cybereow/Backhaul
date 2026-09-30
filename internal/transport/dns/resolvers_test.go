package dnsx

import (
	"reflect"
	"testing"
)

func TestExpandResolvers(t *testing.T) {
	if got := ExpandResolvers(nil); !reflect.DeepEqual(got, DefaultResolvers) {
		t.Errorf("empty list should give the defaults, got %v", got)
	}
	got := ExpandResolvers([]string{"9.9.9.9", "AUTO", "9.9.9.9", DefaultResolvers[0]})
	if got[0] != "9.9.9.9" || len(got) != 1+len(DefaultResolvers) {
		t.Errorf("auto + extras: got %v", got)
	}
	if got := ExpandResolvers([]string{"9.9.9.9"}); !reflect.DeepEqual(got, []string{"9.9.9.9"}) {
		t.Errorf("explicit list must not gain defaults, got %v", got)
	}
}
