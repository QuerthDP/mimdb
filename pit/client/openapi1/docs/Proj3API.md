# \Proj3API

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**CreateTable**](Proj3API.md#CreateTable) | **Put** /table | Create new table in database
[**DeleteTable**](Proj3API.md#DeleteTable) | **Delete** /table/{tableId} | Delete selected table from database
[**GetQueries**](Proj3API.md#GetQueries) | **Get** /queries | Get list of queries (optional in project 3, but useful). Use those IDs to get details by calling /query endpoint.
[**GetQueryById**](Proj3API.md#GetQueryById) | **Get** /query/{queryId} | Get detailed status of selected query
[**GetQueryError**](Proj3API.md#GetQueryError) | **Get** /error/{queryId} | Get error of selected query (will be available only for queries in FAILED state)
[**GetQueryResult**](Proj3API.md#GetQueryResult) | **Get** /result/{queryId} | Get result of selected query (will be available only for SELECT queries after they are completed)
[**GetSystemInfo**](Proj3API.md#GetSystemInfo) | **Get** /system/info | Get basic information about the system (e.g. version, uptime, etc.)
[**GetTableById**](Proj3API.md#GetTableById) | **Get** /table/{tableId} | Get detailed description of selected table
[**GetTables**](Proj3API.md#GetTables) | **Get** /tables | Get list of tables with their accompanying IDs. Use those IDs to get details by calling /table endpoint.
[**SubmitQuery**](Proj3API.md#SubmitQuery) | **Post** /query | Submit new query for execution



## CreateTable

> string CreateTable(ctx).TableSchema(tableSchema).Execute()

Create new table in database

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
	tableSchema := *openapiclient.NewTableSchema("Name_example", []openapiclient.Column{*openapiclient.NewColumn("Name_example", openapiclient.LogicalColumnType("INT64"))}) // TableSchema | Used to create a new table

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.Proj3API.CreateTable(context.Background()).TableSchema(tableSchema).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj3API.CreateTable``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `CreateTable`: string
	fmt.Fprintf(os.Stdout, "Response from `Proj3API.CreateTable`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiCreateTableRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **tableSchema** | [**TableSchema**](TableSchema.md) | Used to create a new table | 

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


## DeleteTable

> DeleteTable(ctx, tableId).Execute()

Delete selected table from database

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
	tableId := "tableId_example" // string | ID of selected Table

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	r, err := apiClient.Proj3API.DeleteTable(context.Background(), tableId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj3API.DeleteTable``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**tableId** | **string** | ID of selected Table | 

### Other Parameters

Other parameters are passed through a pointer to a apiDeleteTableRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


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
	resp, r, err := apiClient.Proj3API.GetQueries(context.Background()).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj3API.GetQueries``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetQueries`: []ShallowQuery
	fmt.Fprintf(os.Stdout, "Response from `Proj3API.GetQueries`: %v\n", resp)
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
	resp, r, err := apiClient.Proj3API.GetQueryById(context.Background(), queryId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj3API.GetQueryById``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetQueryById`: Query
	fmt.Fprintf(os.Stdout, "Response from `Proj3API.GetQueryById`: %v\n", resp)
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
	resp, r, err := apiClient.Proj3API.GetQueryError(context.Background(), queryId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj3API.GetQueryError``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetQueryError`: MultipleProblemsError
	fmt.Fprintf(os.Stdout, "Response from `Proj3API.GetQueryError`: %v\n", resp)
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
	resp, r, err := apiClient.Proj3API.GetQueryResult(context.Background(), queryId).GetQueryResultRequest(getQueryResultRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj3API.GetQueryResult``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetQueryResult`: []QueryResultInner
	fmt.Fprintf(os.Stdout, "Response from `Proj3API.GetQueryResult`: %v\n", resp)
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


## GetSystemInfo

> SystemInformation GetSystemInfo(ctx).Execute()

Get basic information about the system (e.g. version, uptime, etc.)

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
	resp, r, err := apiClient.Proj3API.GetSystemInfo(context.Background()).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj3API.GetSystemInfo``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetSystemInfo`: SystemInformation
	fmt.Fprintf(os.Stdout, "Response from `Proj3API.GetSystemInfo`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiGetSystemInfoRequest struct via the builder pattern


### Return type

[**SystemInformation**](SystemInformation.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetTableById

> TableSchema GetTableById(ctx, tableId).Execute()

Get detailed description of selected table

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
	tableId := "tableId_example" // string | ID of selected Table

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.Proj3API.GetTableById(context.Background(), tableId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj3API.GetTableById``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetTableById`: TableSchema
	fmt.Fprintf(os.Stdout, "Response from `Proj3API.GetTableById`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**tableId** | **string** | ID of selected Table | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetTableByIdRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**TableSchema**](TableSchema.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetTables

> []ShallowTable GetTables(ctx).Execute()

Get list of tables with their accompanying IDs. Use those IDs to get details by calling /table endpoint.

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
	resp, r, err := apiClient.Proj3API.GetTables(context.Background()).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj3API.GetTables``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetTables`: []ShallowTable
	fmt.Fprintf(os.Stdout, "Response from `Proj3API.GetTables`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiGetTablesRequest struct via the builder pattern


### Return type

[**[]ShallowTable**](ShallowTable.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
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
	resp, r, err := apiClient.Proj3API.SubmitQuery(context.Background()).ExecuteQueryRequest(executeQueryRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Proj3API.SubmitQuery``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `SubmitQuery`: string
	fmt.Fprintf(os.Stdout, "Response from `Proj3API.SubmitQuery`: %v\n", resp)
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

