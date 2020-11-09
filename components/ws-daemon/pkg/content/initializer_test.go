package content

import (
	"context"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/sync/errgroup"
)

func TestWriteReceiveMesasge(t *testing.T) {
	type testmsg struct {
		Msg string
	}
	tests := []struct {
		Desc        string
		Input       interface{}
		Output      interface{}
		Expectation interface{}
	}{
		{
			Desc:   "hello world",
			Input:  &testmsg{"Hello World"},
			Output: &testmsg{},
		},
	}

	for _, test := range tests {
		t.Run(test.Desc, func(t *testing.T) {
			pr, pw := io.Pipe()

			eg, _ := errgroup.WithContext(context.Background())
			eg.Go(func() error {
				return writeMessage(pw, test.Input)
			})
			eg.Go(func() error {
				return receiveMessage(pr, test.Output)
			})
			err := eg.Wait()
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.Input, test.Output); diff != "" {
				t.Errorf("unexpected in/ouptut (-want +got):\n%s", diff)
			}
		})
	}
}
