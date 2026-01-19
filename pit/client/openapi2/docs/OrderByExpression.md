# OrderByExpression

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ColumnIndex** | Pointer to **int32** | Index of the column from the columnClauses (0-based). We can Assume that ordering is done only on data that are results of the query. | [optional] 
**Ascending** | Pointer to **bool** |  | [optional] 

## Methods

### NewOrderByExpression

`func NewOrderByExpression() *OrderByExpression`

NewOrderByExpression instantiates a new OrderByExpression object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewOrderByExpressionWithDefaults

`func NewOrderByExpressionWithDefaults() *OrderByExpression`

NewOrderByExpressionWithDefaults instantiates a new OrderByExpression object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetColumnIndex

`func (o *OrderByExpression) GetColumnIndex() int32`

GetColumnIndex returns the ColumnIndex field if non-nil, zero value otherwise.

### GetColumnIndexOk

`func (o *OrderByExpression) GetColumnIndexOk() (*int32, bool)`

GetColumnIndexOk returns a tuple with the ColumnIndex field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetColumnIndex

`func (o *OrderByExpression) SetColumnIndex(v int32)`

SetColumnIndex sets ColumnIndex field to given value.

### HasColumnIndex

`func (o *OrderByExpression) HasColumnIndex() bool`

HasColumnIndex returns a boolean if a field has been set.

### GetAscending

`func (o *OrderByExpression) GetAscending() bool`

GetAscending returns the Ascending field if non-nil, zero value otherwise.

### GetAscendingOk

`func (o *OrderByExpression) GetAscendingOk() (*bool, bool)`

GetAscendingOk returns a tuple with the Ascending field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAscending

`func (o *OrderByExpression) SetAscending(v bool)`

SetAscending sets Ascending field to given value.

### HasAscending

`func (o *OrderByExpression) HasAscending() bool`

HasAscending returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


