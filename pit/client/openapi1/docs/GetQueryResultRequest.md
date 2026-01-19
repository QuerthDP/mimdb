# GetQueryResultRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**RowLimit** | Pointer to **int32** | Maximum number of rows to return | [optional] 
**FlushResult** | Pointer to **bool** | Say to system that result will not be accessed by the user anymore (it is safe to release the resources connected with the result) | [optional] 

## Methods

### NewGetQueryResultRequest

`func NewGetQueryResultRequest() *GetQueryResultRequest`

NewGetQueryResultRequest instantiates a new GetQueryResultRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewGetQueryResultRequestWithDefaults

`func NewGetQueryResultRequestWithDefaults() *GetQueryResultRequest`

NewGetQueryResultRequestWithDefaults instantiates a new GetQueryResultRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetRowLimit

`func (o *GetQueryResultRequest) GetRowLimit() int32`

GetRowLimit returns the RowLimit field if non-nil, zero value otherwise.

### GetRowLimitOk

`func (o *GetQueryResultRequest) GetRowLimitOk() (*int32, bool)`

GetRowLimitOk returns a tuple with the RowLimit field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRowLimit

`func (o *GetQueryResultRequest) SetRowLimit(v int32)`

SetRowLimit sets RowLimit field to given value.

### HasRowLimit

`func (o *GetQueryResultRequest) HasRowLimit() bool`

HasRowLimit returns a boolean if a field has been set.

### GetFlushResult

`func (o *GetQueryResultRequest) GetFlushResult() bool`

GetFlushResult returns the FlushResult field if non-nil, zero value otherwise.

### GetFlushResultOk

`func (o *GetQueryResultRequest) GetFlushResultOk() (*bool, bool)`

GetFlushResultOk returns a tuple with the FlushResult field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFlushResult

`func (o *GetQueryResultRequest) SetFlushResult(v bool)`

SetFlushResult sets FlushResult field to given value.

### HasFlushResult

`func (o *GetQueryResultRequest) HasFlushResult() bool`

HasFlushResult returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


