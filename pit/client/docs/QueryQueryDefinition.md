# QueryQueryDefinition

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ColumnClauses** | [**[]ColumnExpression**](ColumnExpression.md) |  | 
**WhereClause** | Pointer to [**ColumnExpression**](ColumnExpression.md) |  | [optional] 
**OrderByClause** | Pointer to [**[]OrderByExpression**](OrderByExpression.md) |  | [optional] 
**LimitClause** | Pointer to [**LimitExpression**](LimitExpression.md) |  | [optional] 
**SourceFilepath** | **string** | Path to source CSV file (filepath in perspective of running server! NOT client) | 
**DestinationTableName** | **string** |  | 
**DestinationColumns** | Pointer to **[]string** | List of columns to copy data into. It creates a map from source columns to destination columns. Assumes that data in source file is in the same order as in this list. | [optional] 
**DoesCsvContainHeader** | Pointer to **bool** | Whether CSV file contains header row | [optional] [default to false]

## Methods

### NewQueryQueryDefinition

`func NewQueryQueryDefinition(columnClauses []ColumnExpression, sourceFilepath string, destinationTableName string, ) *QueryQueryDefinition`

NewQueryQueryDefinition instantiates a new QueryQueryDefinition object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewQueryQueryDefinitionWithDefaults

`func NewQueryQueryDefinitionWithDefaults() *QueryQueryDefinition`

NewQueryQueryDefinitionWithDefaults instantiates a new QueryQueryDefinition object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetColumnClauses

`func (o *QueryQueryDefinition) GetColumnClauses() []ColumnExpression`

GetColumnClauses returns the ColumnClauses field if non-nil, zero value otherwise.

### GetColumnClausesOk

`func (o *QueryQueryDefinition) GetColumnClausesOk() (*[]ColumnExpression, bool)`

GetColumnClausesOk returns a tuple with the ColumnClauses field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetColumnClauses

`func (o *QueryQueryDefinition) SetColumnClauses(v []ColumnExpression)`

SetColumnClauses sets ColumnClauses field to given value.


### GetWhereClause

`func (o *QueryQueryDefinition) GetWhereClause() ColumnExpression`

GetWhereClause returns the WhereClause field if non-nil, zero value otherwise.

### GetWhereClauseOk

`func (o *QueryQueryDefinition) GetWhereClauseOk() (*ColumnExpression, bool)`

GetWhereClauseOk returns a tuple with the WhereClause field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetWhereClause

`func (o *QueryQueryDefinition) SetWhereClause(v ColumnExpression)`

SetWhereClause sets WhereClause field to given value.

### HasWhereClause

`func (o *QueryQueryDefinition) HasWhereClause() bool`

HasWhereClause returns a boolean if a field has been set.

### GetOrderByClause

`func (o *QueryQueryDefinition) GetOrderByClause() []OrderByExpression`

GetOrderByClause returns the OrderByClause field if non-nil, zero value otherwise.

### GetOrderByClauseOk

`func (o *QueryQueryDefinition) GetOrderByClauseOk() (*[]OrderByExpression, bool)`

GetOrderByClauseOk returns a tuple with the OrderByClause field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOrderByClause

`func (o *QueryQueryDefinition) SetOrderByClause(v []OrderByExpression)`

SetOrderByClause sets OrderByClause field to given value.

### HasOrderByClause

`func (o *QueryQueryDefinition) HasOrderByClause() bool`

HasOrderByClause returns a boolean if a field has been set.

### GetLimitClause

`func (o *QueryQueryDefinition) GetLimitClause() LimitExpression`

GetLimitClause returns the LimitClause field if non-nil, zero value otherwise.

### GetLimitClauseOk

`func (o *QueryQueryDefinition) GetLimitClauseOk() (*LimitExpression, bool)`

GetLimitClauseOk returns a tuple with the LimitClause field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLimitClause

`func (o *QueryQueryDefinition) SetLimitClause(v LimitExpression)`

SetLimitClause sets LimitClause field to given value.

### HasLimitClause

`func (o *QueryQueryDefinition) HasLimitClause() bool`

HasLimitClause returns a boolean if a field has been set.

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


