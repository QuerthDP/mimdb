# ExecuteQueryRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**QueryDefinition** | [**QueryQueryDefinition**](QueryQueryDefinition.md) |  | 

## Methods

### NewExecuteQueryRequest

`func NewExecuteQueryRequest(queryDefinition QueryQueryDefinition, ) *ExecuteQueryRequest`

NewExecuteQueryRequest instantiates a new ExecuteQueryRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewExecuteQueryRequestWithDefaults

`func NewExecuteQueryRequestWithDefaults() *ExecuteQueryRequest`

NewExecuteQueryRequestWithDefaults instantiates a new ExecuteQueryRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetQueryDefinition

`func (o *ExecuteQueryRequest) GetQueryDefinition() QueryQueryDefinition`

GetQueryDefinition returns the QueryDefinition field if non-nil, zero value otherwise.

### GetQueryDefinitionOk

`func (o *ExecuteQueryRequest) GetQueryDefinitionOk() (*QueryQueryDefinition, bool)`

GetQueryDefinitionOk returns a tuple with the QueryDefinition field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetQueryDefinition

`func (o *ExecuteQueryRequest) SetQueryDefinition(v QueryQueryDefinition)`

SetQueryDefinition sets QueryDefinition field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


