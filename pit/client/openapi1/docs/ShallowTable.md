# ShallowTable

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**TableId** | Pointer to **string** | ID of selected Table (I propose UUID, but it is under your own discretion) | [optional] 
**Name** | **string** |  | 

## Methods

### NewShallowTable

`func NewShallowTable(name string, ) *ShallowTable`

NewShallowTable instantiates a new ShallowTable object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewShallowTableWithDefaults

`func NewShallowTableWithDefaults() *ShallowTable`

NewShallowTableWithDefaults instantiates a new ShallowTable object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetTableId

`func (o *ShallowTable) GetTableId() string`

GetTableId returns the TableId field if non-nil, zero value otherwise.

### GetTableIdOk

`func (o *ShallowTable) GetTableIdOk() (*string, bool)`

GetTableIdOk returns a tuple with the TableId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTableId

`func (o *ShallowTable) SetTableId(v string)`

SetTableId sets TableId field to given value.

### HasTableId

`func (o *ShallowTable) HasTableId() bool`

HasTableId returns a boolean if a field has been set.

### GetName

`func (o *ShallowTable) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *ShallowTable) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *ShallowTable) SetName(v string)`

SetName sets Name field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


