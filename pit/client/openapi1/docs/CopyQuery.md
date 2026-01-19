# CopyQuery

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**SourceFilepath** | **string** | Path to source CSV file (filepath in perspective of running server! NOT client) | 
**DestinationTableName** | **string** |  | 
**DestinationColumns** | Pointer to **[]string** | List of columns to copy data into. It creates a map from source columns to destination columns. Assumes that data in source file is in the same order as in this list. | [optional] 
**DoesCsvContainHeader** | Pointer to **bool** | Whether CSV file contains header row | [optional] [default to false]

## Methods

### NewCopyQuery

`func NewCopyQuery(sourceFilepath string, destinationTableName string, ) *CopyQuery`

NewCopyQuery instantiates a new CopyQuery object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewCopyQueryWithDefaults

`func NewCopyQueryWithDefaults() *CopyQuery`

NewCopyQueryWithDefaults instantiates a new CopyQuery object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetSourceFilepath

`func (o *CopyQuery) GetSourceFilepath() string`

GetSourceFilepath returns the SourceFilepath field if non-nil, zero value otherwise.

### GetSourceFilepathOk

`func (o *CopyQuery) GetSourceFilepathOk() (*string, bool)`

GetSourceFilepathOk returns a tuple with the SourceFilepath field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSourceFilepath

`func (o *CopyQuery) SetSourceFilepath(v string)`

SetSourceFilepath sets SourceFilepath field to given value.


### GetDestinationTableName

`func (o *CopyQuery) GetDestinationTableName() string`

GetDestinationTableName returns the DestinationTableName field if non-nil, zero value otherwise.

### GetDestinationTableNameOk

`func (o *CopyQuery) GetDestinationTableNameOk() (*string, bool)`

GetDestinationTableNameOk returns a tuple with the DestinationTableName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDestinationTableName

`func (o *CopyQuery) SetDestinationTableName(v string)`

SetDestinationTableName sets DestinationTableName field to given value.


### GetDestinationColumns

`func (o *CopyQuery) GetDestinationColumns() []string`

GetDestinationColumns returns the DestinationColumns field if non-nil, zero value otherwise.

### GetDestinationColumnsOk

`func (o *CopyQuery) GetDestinationColumnsOk() (*[]string, bool)`

GetDestinationColumnsOk returns a tuple with the DestinationColumns field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDestinationColumns

`func (o *CopyQuery) SetDestinationColumns(v []string)`

SetDestinationColumns sets DestinationColumns field to given value.

### HasDestinationColumns

`func (o *CopyQuery) HasDestinationColumns() bool`

HasDestinationColumns returns a boolean if a field has been set.

### GetDoesCsvContainHeader

`func (o *CopyQuery) GetDoesCsvContainHeader() bool`

GetDoesCsvContainHeader returns the DoesCsvContainHeader field if non-nil, zero value otherwise.

### GetDoesCsvContainHeaderOk

`func (o *CopyQuery) GetDoesCsvContainHeaderOk() (*bool, bool)`

GetDoesCsvContainHeaderOk returns a tuple with the DoesCsvContainHeader field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDoesCsvContainHeader

`func (o *CopyQuery) SetDoesCsvContainHeader(v bool)`

SetDoesCsvContainHeader sets DoesCsvContainHeader field to given value.

### HasDoesCsvContainHeader

`func (o *CopyQuery) HasDoesCsvContainHeader() bool`

HasDoesCsvContainHeader returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


