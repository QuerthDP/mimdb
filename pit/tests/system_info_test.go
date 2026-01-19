package tests

import (
	"context"
	"net/http"
	"testing"

	"github.com/smogork/ISBD-MIMUW/pit"
	"github.com/stretchr/testify/require"
)

func TestSystemInfo(t *testing.T) {
	apiClient := pit.DbClient1(BaseURL)

	RunTracked(t, "SystemInfo", func(t *testing.T) {
		ctx := context.Background()

		// Log the request
		t.Log("Sending request:\nGET /system")
		sysInfo, resp, err := apiClient.MetadataAPI.GetSystemInfo(ctx).Execute()

		// Log the raw HTTP response for debugging / validation
		t.Log(pit.FormatResponse(resp))

		require.NoError(t, err, "GetSystemInfo should not return an error")
		require.Equal(t, http.StatusOK, resp.StatusCode, "GetSystemInfo should return status 200")
		require.NotNil(t, sysInfo, "SystemInformation should not be nil")
		require.NotEmpty(t, sysInfo.Version, "Version field should not be empty")
		require.NotEmpty(t, sysInfo.Author, "Author field should not be empty")
	})
}
