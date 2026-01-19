# Column

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Type** | [**LogicalColumnType**](LogicalColumnType.md) |  | 

## Methods

### NewColumn

`func NewColumn(name string, type_ LogicalColumnType, ) *Column`

NewColumn instantiates a new Column object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewColumnWithDefaults

`func NewColumnWithDefaults() *Column`

NewColumnWithDefaults instantiates a new Column object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *Column) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *Column) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *Column) SetName(v string)`

SetName sets Name field to given value.


### GetType

`func (o *Column) GetType() LogicalColumnType`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *Column) GetTypeOk() (*LogicalColumnType, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *Column) SetType(v LogicalColumnType)`

SetType sets Type field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


