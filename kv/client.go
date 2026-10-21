package kv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Client talks to the cluster over HTTP, follows leader redirects, and
// retries with the same sequence number so each write applies once. A Client
// is a single session: use one per goroutine.
type Client struct {
	Nodes   []string // host:port of every node
	HTTP    *http.Client
	Attempt time.Duration // per-request timeout

	id     uint64
	seq    uint64
	leader string
}

func NewClient(nodes []string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}}
	}
	return &Client{Nodes: nodes, HTTP: hc, Attempt: time.Second, id: rand.Uint64() | 1}
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, http.MethodPut, key, "", value)
	return err
}

func (c *Client) Append(ctx context.Context, key, value string) (string, error) {
	return c.do(ctx, http.MethodPost, key, "?op=append", value)
}

// Get returns ErrKeyNotFound for a missing key.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.do(ctx, http.MethodGet, key, "", "")
}

var ErrKeyNotFound = errors.New("kv: key not found")

// Leader returns the node the client last saw acting as leader.
func (c *Client) Leader() string { return c.leader }

func (c *Client) do(ctx context.Context, method, key, query, body string) (string, error) {
	c.seq++
	var lastErr error
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				lastErr = err
			}
			return "", fmt.Errorf("%w (last error: %v)", context.DeadlineExceeded, lastErr)
		}
		target := c.leader
		if target == "" {
			target = c.Nodes[rand.Intn(len(c.Nodes))]
		}
		val, status, hint, err := c.once(ctx, method, target, key, query, body)
		switch {
		case err == nil && (status == http.StatusOK):
			c.leader = target
			return val, nil
		case err == nil && status == http.StatusNotFound:
			c.leader = target
			return "", ErrKeyNotFound
		case err == nil && status == http.StatusMisdirectedRequest && hint != "" && hint != target:
			c.leader = hint
			lastErr = fmt.Errorf("redirected to %s", hint)
			continue
		case err == nil:
			lastErr = fmt.Errorf("%s: HTTP %d: %s", target, status, strings.TrimSpace(val))
		default:
			lastErr = err
		}
		c.leader = ""
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
}

func (c *Client) once(ctx context.Context, method, node, key, query, body string) (string, int, string, error) {
	actx, cancel := context.WithTimeout(ctx, c.Attempt)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, method, "http://"+node+"/kv/"+key+query, strings.NewReader(body))
	if err != nil {
		return "", 0, "", err
	}
	req.Header.Set("X-Client-ID", strconv.FormatUint(c.id, 10))
	req.Header.Set("X-Seq", strconv.FormatUint(c.seq, 10))
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", 0, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, "", err
	}
	return string(b), resp.StatusCode, resp.Header.Get("X-Leader"), nil
}
