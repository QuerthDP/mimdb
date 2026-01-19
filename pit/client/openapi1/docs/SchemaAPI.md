# \SchemaAPI

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**CreateTable**](SchemaAPI.md#CreateTable) | **Put** /table | Create new table in database
[**DeleteTable**](SchemaAPI.md#DeleteTable) | **Delete** /table/{tableId} | Delete selected table from database
[**GetTableById**](SchemaAPI.md#GetTableById) | **Get** /table/{tableId} | Get detailed description of selected table
[**GetTables**](SchemaAPI.md#GetTables) | **Get** /tables | Get list of tables with their accompanying IDs. Use those IDs to get details by calling /table endpoint.



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
	resp, r, err := apiClient.SchemaAPI.CreateTable(context.Background()).TableSchema(tableSchema).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `SchemaAPI.CreateTable``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `CreateTable`: string
	fmt.Fprintf(os.Stdout, "Response from `SchemaAPI.CreateTable`: %v\n", resp)
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
	r, err := apiClient.SchemaAPI.DeleteTable(context.Background(), tableId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `SchemaAPI.DeleteTable``: %v\n", err)
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
	resp, r, err := apiClient.SchemaAPI.GetTableById(context.Background(), tableId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `SchemaAPI.GetTableById``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetTableById`: TableSchema
	fmt.Fprintf(os.Stdout, "Response from `SchemaAPI.GetTableById`: %v\n", resp)
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
	resp, r, err := apiClient.SchemaAPI.GetTables(context.Background()).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `SchemaAPI.GetTables``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetTables`: []ShallowTable
	fmt.Fprintf(os.Stdout, "Response from `SchemaAPI.GetTables`: %v\n", resp)
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

