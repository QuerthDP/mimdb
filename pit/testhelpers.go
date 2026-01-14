package pit

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	apiclient "github.com/smogork/ISBD-MIMUW/pit/client"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func DbClient(url string) *apiclient.APIClient {
	// Create and configure API client
	cfg := apiclient.NewConfiguration()
	cfg.Servers[0].URL = url
	cfg.HTTPClient = &http.Client{}
	return apiclient.NewAPIClient(cfg)
}

func waitForHTTP(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{}
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		resp, err := client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", url)
}

// StartTestContainer starts the test container (unless DB_RUN_DOCKER is set), waits for
// it to become healthy and returns the base URL and a teardown function.
//
// Configuration via environment variables:
// - DB_RUN_DOCKER: if set to anything other than "false", skip docker and connect to existing database
// - DB_IMAGE: docker image name (default: "isbd-mimuw-db:latest")
// - DB_HOSTNAME: hostname of running database (default: "localhost")
// - DB_PORT: port on which database listens (default: "8080")
func StartTestContainer(ctx context.Context) (string, func(), error) {
	dbRunDocker := os.Getenv("DB_RUN_DOCKER")
	if dbRunDocker == "" {
		dbRunDocker = "false" // By default use already running DB
	}

	if dbRunDocker == "false" {
		// Connect to existing database without starting container
		hostname := os.Getenv("DB_HOSTNAME")
		if hostname == "" {
			hostname = "localhost"
		}
		port := os.Getenv("DB_PORT")
		if port == "" {
			port = "8080"
		}

		base := fmt.Sprintf("http://%s:%s", hostname, port)
		if err := waitForHTTP(base+"/system/info", 30*time.Second); err != nil {
			return "", nil, err
		}
		return base, func() {}, nil
	}

	// Start docker container
	image := os.Getenv("DB_IMAGE")
	if image == "" {
		image = "isbd-mimuw-db:latest"
	}

	req := tc.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"8080/tcp"},
		WaitingFor:   wait.ForHTTP("/system/info").WithPort("8080/tcp").WithStartupTimeout(30 * time.Second),
	}

	cont, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return "", nil, err
	}

	host, err := cont.Host(ctx)
	if err != nil {
		_ = cont.Terminate(ctx)
		return "", nil, err
	}
	mp, err := cont.MappedPort(ctx, "8080/tcp")
	if err != nil {
		_ = cont.Terminate(ctx)
		return "", nil, err
	}
	base := fmt.Sprintf("http://%s:%s", host, mp.Port())

	// Wait for system info to be available (uses waitForHTTP defined in this package)
	if err := waitForHTTP(base+"/system/info", 30*time.Second); err != nil {
		_ = cont.Terminate(ctx)
		return "", nil, err
	}

	teardown := func() { _ = cont.Terminate(ctx) }
	return base, teardown, nil
}
