# ShallowQuery

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**QueryId** | **string** | ID of selected Query (I propose UUID, but it is under your own discretion) | 
**Status** | [**QueryStatus**](QueryStatus.md) |  | 

## Methods

### NewShallowQuery

`func NewShallowQuery(queryId string, status QueryStatus, ) *ShallowQuery`

NewShallowQuery instantiates a new ShallowQuery object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewShallowQueryWithDefaults

`func NewShallowQueryWithDefaults() *ShallowQuery`

NewShallowQueryWithDefaults instantiates a new ShallowQuery object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetQueryId

`func (o *ShallowQuery) GetQueryId() string`

GetQueryId returns the QueryId field if non-nil, zero value otherwise.

### GetQueryIdOk

`func (o *ShallowQuery) GetQueryIdOk() (*string, bool)`

GetQueryIdOk returns a tuple with the QueryId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetQueryId

`func (o *ShallowQuery) SetQueryId(v string)`

SetQueryId sets QueryId field to given value.


### GetStatus

`func (o *ShallowQuery) GetStatus() QueryStatus`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *ShallowQuery) GetStatusOk() (*QueryStatus, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *ShallowQuery) SetStatus(v QueryStatus)`

SetStatus sets Status field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


