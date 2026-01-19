# \Proj4API

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GetQueries**](Proj4API.md#GetQueries) | **Get** /queries | Get list of queries (optional in project 3, but useful). Use those IDs to get details by calling /query endpoint.



## GetQueries

> []ShallowQuery GetQueries(ctx).Execute()

Get list of queries (optional in project 3, but useful). Use those IDs to get details by calling /query endpoint.

### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.Proj4API.GetQueries(context.Background()).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj4API.GetQueries``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetQueries`: []ShallowQuery
	fmt.Fprintf(os.Stdout, "Response from `Proj4API.GetQueries`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiGetQueriesRequest struct via the builder pattern


### Return type

[**[]ShallowQuery**](ShallowQuery.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

