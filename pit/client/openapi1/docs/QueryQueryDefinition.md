# QueryQueryDefinition

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**TableName** | Pointer to **string** |  | [optional] 
**SourceFilepath** | **string** | Path to source CSV file (filepath in perspective of running server! NOT client) | 
**DestinationTableName** | **string** |  | 
**DestinationColumns** | Pointer to **[]string** | List of columns to copy data into. It creates a map from source columns to destination columns. Assumes that data in source file is in the same order as in this list. | [optional] 
**DoesCsvContainHeader** | Pointer to **bool** | Whether CSV file contains header row | [optional] [default to false]

## Methods

### NewQueryQueryDefinition

`func NewQueryQueryDefinition(sourceFilepath string, destinationTableName string, ) *QueryQueryDefinition`

NewQueryQueryDefinition instantiates a new QueryQueryDefinition object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewQueryQueryDefinitionWithDefaults

`func NewQueryQueryDefinitionWithDefaults() *QueryQueryDefinition`

NewQueryQueryDefinitionWithDefaults instantiates a new QueryQueryDefinition object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetTableName

`func (o *QueryQueryDefinition) GetTableName() string`

GetTableName returns the TableName field if non-nil, zero value otherwise.

### GetTableNameOk

`func (o *QueryQueryDefinition) GetTableNameOk() (*string, bool)`

GetTableNameOk returns a tuple with the TableName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTableName

`func (o *QueryQueryDefinition) SetTableName(v string)`

SetTableName sets TableName field to given value.

### HasTableName

`func (o *QueryQueryDefinition) HasTableName() bool`

HasTableName returns a boolean if a field has been set.

### GetSourceFilepath

`func (o *QueryQueryDefinition) GetSourceFilepath() string`

GetSourceFilepath returns the SourceFilepath field if non-nil, zero value otherwise.

### GetSourceFilepathOk

`func (o *QueryQueryDefinition) GetSourceFilepathOk() (*string, bool)`

GetSourceFilepathOk returns a tuple with the SourceFilepath field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSourceFilepath

`func (o *QueryQueryDefinition) SetSourceFilepath(v string)`

SetSourceFilepath sets SourceFilepath field to given value.


### GetDestinationTableName

`func (o *QueryQueryDefinition) GetDestinationTableName() string`

GetDestinationTableName returns the DestinationTableName field if non-nil, zero value otherwise.

### GetDestinationTableNameOk

`func (o *QueryQueryDefinition) GetDestinationTableNameOk() (*string, bool)`

GetDestinationTableNameOk returns a tuple with the DestinationTableName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDestinationTableName

`func (o *QueryQueryDefinition) SetDestinationTableName(v string)`

SetDestinationTableName sets DestinationTableName field to given value.


### GetDestinationColumns

`func (o *QueryQueryDefinition) GetDestinationColumns() []string`

GetDestinationColumns returns the DestinationColumns field if non-nil, zero value otherwise.

### GetDestinationColumnsOk

`func (o *QueryQueryDefinition) GetDestinationColumnsOk() (*[]string, bool)`

GetDestinationColumnsOk returns a tuple with the DestinationColumns field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDestinationColumns

`func (o *QueryQueryDefinition) SetDestinationColumns(v []string)`

SetDestinationColumns sets DestinationColumns field to given value.

### HasDestinationColumns

`func (o *QueryQueryDefinition) HasDestinationColumns() bool`

HasDestinationColumns returns a boolean if a field has been set.

### GetDoesCsvContainHeader

`func (o *QueryQueryDefinition) GetDoesCsvContainHeader() bool`

GetDoesCsvContainHeader returns the DoesCsvContainHeader field if non-nil, zero value otherwise.

### GetDoesCsvContainHeaderOk

`func (o *QueryQueryDefinition) GetDoesCsvContainHeaderOk() (*bool, bool)`

GetDoesCsvContainHeaderOk returns a tuple with the DoesCsvContainHeader field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDoesCsvContainHeader

`func (o *QueryQueryDefinition) SetDoesCsvContainHeader(v bool)`

SetDoesCsvContainHeader sets DoesCsvContainHeader field to given value.

### HasDoesCsvContainHeader

`func (o *QueryQueryDefinition) HasDoesCsvContainHeader() bool`

HasDoesCsvContainHeader returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


