# Query

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**QueryId** | **string** | ID of selected Query (I propose UUID, but it is under your own discretion) | 
**Status** | [**QueryStatus**](QueryStatus.md) |  | 
**IsResultAvailable** | Pointer to **bool** | Whether result of this query is already available | [optional] 
**QueryDefinition** | Pointer to [**QueryQueryDefinition**](QueryQueryDefinition.md) |  | [optional] 

## Methods

### NewQuery

`func NewQuery(queryId string, status QueryStatus, ) *Query`

NewQuery instantiates a new Query object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewQueryWithDefaults

`func NewQueryWithDefaults() *Query`

NewQueryWithDefaults instantiates a new Query object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetQueryId

`func (o *Query) GetQueryId() string`

GetQueryId returns the QueryId field if non-nil, zero value otherwise.

### GetQueryIdOk

`func (o *Query) GetQueryIdOk() (*string, bool)`

GetQueryIdOk returns a tuple with the QueryId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetQueryId

`func (o *Query) SetQueryId(v string)`

SetQueryId sets QueryId field to given value.


### GetStatus

`func (o *Query) GetStatus() QueryStatus`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *Query) GetStatusOk() (*QueryStatus, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *Query) SetStatus(v QueryStatus)`

SetStatus sets Status field to given value.


### GetIsResultAvailable

`func (o *Query) GetIsResultAvailable() bool`

GetIsResultAvailable returns the IsResultAvailable field if non-nil, zero value otherwise.

### GetIsResultAvailableOk

`func (o *Query) GetIsResultAvailableOk() (*bool, bool)`

GetIsResultAvailableOk returns a tuple with the IsResultAvailable field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetIsResultAvailable

`func (o *Query) SetIsResultAvailable(v bool)`

SetIsResultAvailable sets IsResultAvailable field to given value.

### HasIsResultAvailable

`func (o *Query) HasIsResultAvailable() bool`

HasIsResultAvailable returns a boolean if a field has been set.

### GetQueryDefinition

`func (o *Query) GetQueryDefinition() QueryQueryDefinition`

GetQueryDefinition returns the QueryDefinition field if non-nil, zero value otherwise.

### GetQueryDefinitionOk

`func (o *Query) GetQueryDefinitionOk() (*QueryQueryDefinition, bool)`

GetQueryDefinitionOk returns a tuple with the QueryDefinition field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetQueryDefinition

`func (o *Query) SetQueryDefinition(v QueryQueryDefinition)`

SetQueryDefinition sets QueryDefinition field to given value.

### HasQueryDefinition

`func (o *Query) HasQueryDefinition() bool`

HasQueryDefinition returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


