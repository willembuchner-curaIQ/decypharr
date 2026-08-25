package nntp

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/testutil/nntpd"
)

func TestBatchStatAllChecksEveryArticle(t *testing.T) {
	server, err := nntpd.New(nntpd.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)

	const missing = "<missing@nntpd>"
	messageIDs := make([]string, 25)
	messageIDs[0] = missing
	for i := 1; i < len(messageIDs); i++ {
		messageIDs[i] = fmt.Sprintf("<article-%d@nntpd>", i)
		server.AddArticle(messageIDs[i], []byte("article"))
	}

	host, port := server.Addr()
	client, err := NewClient(&config.Config{
		Usenet: config.Usenet{Providers: []config.UsenetProvider{{
			Host: host, Port: port, MaxConnections: 1,
		}}},
		Repair: config.RepairConfig{NNTPConnectionPercent: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	result, err := client.BatchStatAll(context.Background(), messageIDs)
	if err != nil {
		t.Fatalf("BatchStatAll: %v", err)
	}
	if result.TotalCount != len(messageIDs) || result.FoundCount != len(messageIDs)-1 || result.ErrorCount != 0 {
		t.Fatalf("unexpected counts: %+v", result)
	}
	for i, article := range result.Results {
		if article.MessageID != messageIDs[i] {
			t.Fatalf("result %d message ID=%q, want %q", i, article.MessageID, messageIDs[i])
		}
		if i == 0 {
			if article.Available || !IsArticleNotFoundError(article.Error) {
				t.Fatalf("missing result=%+v", article)
			}
			continue
		}
		if !article.Available || article.Error != nil {
			t.Fatalf("available result %d=%+v", i, article)
		}
	}
}

func TestBackgroundOperationsUseRepairPoolCapacity(t *testing.T) {
	server, err := nntpd.New(nntpd.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	host, port := server.Addr()
	client, err := NewClient(&config.Config{
		Usenet: config.Usenet{Providers: []config.UsenetProvider{{
			Host: host, Port: port, MaxConnections: 1,
		}}},
		Repair: config.RepairConfig{NNTPConnectionPercent: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	errors := make(chan error, 2)
	for range 2 {
		go func() {
			errors <- client.ExecuteBackgroundWithFailover(context.Background(), func(*Connection) error {
				entered <- struct{}{}
				<-release
				return nil
			})
		}()
	}

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first background operation did not start")
	}
	select {
	case <-entered:
		t.Fatal("repair pool ran two background operations with capacity one")
	case <-time.After(50 * time.Millisecond):
	}
	releaseAll()
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
}
