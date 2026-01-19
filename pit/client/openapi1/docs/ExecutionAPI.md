# \ExecutionAPI

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GetQueries**](ExecutionAPI.md#GetQueries) | **Get** /queries | Get list of queries (optional in project 3, but useful). Use those IDs to get details by calling /query endpoint.
[**GetQueryById**](ExecutionAPI.md#GetQueryById) | **Get** /query/{queryId} | Get detailed status of selected query
[**GetQueryError**](ExecutionAPI.md#GetQueryError) | **Get** /error/{queryId} | Get error of selected query (will be available only for queries in FAILED state)
[**GetQueryResult**](ExecutionAPI.md#GetQueryResult) | **Get** /result/{queryId} | Get result of selected query (will be available only for SELECT queries after they are completed)
[**SubmitQuery**](ExecutionAPI.md#SubmitQuery) | **Post** /query | Submit new query for execution



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
	resp, r, err := apiClient.ExecutionAPI.GetQueries(context.Background()).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ExecutionAPI.GetQueries``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetQueries`: []ShallowQuery
	fmt.Fprintf(os.Stdout, "Response from `ExecutionAPI.GetQueries`: %v\n", resp)
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


## GetQueryById

> Query GetQueryById(ctx, queryId).Execute()

Get detailed status of selected query

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
	queryId := "queryId_example" // string | ID of selected Query

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ExecutionAPI.GetQueryById(context.Background(), queryId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ExecutionAPI.GetQueryById``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetQueryById`: Query
	fmt.Fprintf(os.Stdout, "Response from `ExecutionAPI.GetQueryById`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**queryId** | **string** | ID of selected Query | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetQueryByIdRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**Query**](Query.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetQueryError

> MultipleProblemsError GetQueryError(ctx, queryId).Execute()

Get error of selected query (will be available only for queries in FAILED state)

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
	queryId := "queryId_example" // string | ID of selected Query

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ExecutionAPI.GetQueryError(context.Background(), queryId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ExecutionAPI.GetQueryError``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetQueryError`: MultipleProblemsError
	fmt.Fprintf(os.Stdout, "Response from `ExecutionAPI.GetQueryError`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**queryId** | **string** | ID of selected Query | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetQueryErrorRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**MultipleProblemsError**](MultipleProblemsError.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetQueryResult

> []QueryResultInner GetQueryResult(ctx, queryId).GetQueryResultRequest(getQueryResultRequest).Execute()

Get result of selected query (will be available only for SELECT queries after they are completed)

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
	queryId := "queryId_example" // string | ID of selected Query
	getQueryResultRequest := *openapiclient.NewGetQueryResultRequest() // GetQueryResultRequest | Used to get result of a query (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ExecutionAPI.GetQueryResult(context.Background(), queryId).GetQueryResultRequest(getQueryResultRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ExecutionAPI.GetQueryResult``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetQueryResult`: []QueryResultInner
	fmt.Fprintf(os.Stdout, "Response from `ExecutionAPI.GetQueryResult`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**queryId** | **string** | ID of selected Query | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetQueryResultRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **getQueryResultRequest** | [**GetQueryResultRequest**](GetQueryResultRequest.md) | Used to get result of a query | 

### Return type

[**[]QueryResultInner**](QueryResultInner.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## SubmitQuery

> string SubmitQuery(ctx).ExecuteQueryRequest(executeQueryRequest).Execute()

Submit new query for execution

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
	executeQueryRequest := *openapiclient.NewExecuteQueryRequest(openapiclient.Query_queryDefinition{CopyQuery: openapiclient.NewCopyQuery("SourceFilepath_example", "DestinationTableName_example")}) // ExecuteQueryRequest | Used to submit a new query for execution

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ExecutionAPI.SubmitQuery(context.Background()).ExecuteQueryRequest(executeQueryRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ExecutionAPI.SubmitQuery``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `SubmitQuery`: string
	fmt.Fprintf(os.Stdout, "Response from `ExecutionAPI.SubmitQuery`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiSubmitQueryRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **executeQueryRequest** | [**ExecuteQueryRequest**](ExecuteQueryRequest.md) | Used to submit a new query for execution | 

### Return type

**string**

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

