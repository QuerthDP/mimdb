# TableSchema

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Columns** | [**[]Column**](Column.md) |  | 

## Methods

### NewTableSchema

`func NewTableSchema(name string, columns []Column, ) *TableSchema`

NewTableSchema instantiates a new TableSchema object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTableSchemaWithDefaults

`func NewTableSchemaWithDefaults() *TableSchema`

NewTableSchemaWithDefaults instantiates a new TableSchema object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *TableSchema) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *TableSchema) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *TableSchema) SetName(v string)`

SetName sets Name field to given value.


### GetColumns

`func (o *TableSchema) GetColumns() []Column`

GetColumns returns the Columns field if non-nil, zero value otherwise.

### GetColumnsOk

`func (o *TableSchema) GetColumnsOk() (*[]Column, bool)`

GetColumnsOk returns a tuple with the Columns field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetColumns

`func (o *TableSchema) SetColumns(v []Column)`

SetColumns sets Columns field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


