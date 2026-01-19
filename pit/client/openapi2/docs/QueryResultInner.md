# QueryResultInner

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**RowCount** | Pointer to **int32** | Number of rows in result | [optional] 
**Columns** | Pointer to [**[]QueryResultInnerColumnsInner**](QueryResultInnerColumnsInner.md) | Array of columns in result (all should have the same length equal to rowCount) | [optional] 

## Methods

### NewQueryResultInner

`func NewQueryResultInner() *QueryResultInner`

NewQueryResultInner instantiates a new QueryResultInner object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewQueryResultInnerWithDefaults

`func NewQueryResultInnerWithDefaults() *QueryResultInner`

NewQueryResultInnerWithDefaults instantiates a new QueryResultInner object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetRowCount

`func (o *QueryResultInner) GetRowCount() int32`

GetRowCount returns the RowCount field if non-nil, zero value otherwise.

### GetRowCountOk

`func (o *QueryResultInner) GetRowCountOk() (*int32, bool)`

GetRowCountOk returns a tuple with the RowCount field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRowCount

`func (o *QueryResultInner) SetRowCount(v int32)`

SetRowCount sets RowCount field to given value.

### HasRowCount

`func (o *QueryResultInner) HasRowCount() bool`

HasRowCount returns a boolean if a field has been set.

### GetColumns

`func (o *QueryResultInner) GetColumns() []QueryResultInnerColumnsInner`

GetColumns returns the Columns field if non-nil, zero value otherwise.

### GetColumnsOk

`func (o *QueryResultInner) GetColumnsOk() (*[]QueryResultInnerColumnsInner, bool)`

GetColumnsOk returns a tuple with the Columns field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetColumns

`func (o *QueryResultInner) SetColumns(v []QueryResultInnerColumnsInner)`

SetColumns sets Columns field to given value.

### HasColumns

`func (o *QueryResultInner) HasColumns() bool`

HasColumns returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


