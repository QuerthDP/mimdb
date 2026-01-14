# SelectQuery

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ColumnClauses** | [**[]ColumnExpression**](ColumnExpression.md) |  | 
**WhereClause** | Pointer to [**ColumnExpression**](ColumnExpression.md) |  | [optional] 
**OrderByClause** | Pointer to [**[]OrderByExpression**](OrderByExpression.md) |  | [optional] 
**LimitClause** | Pointer to [**LimitExpression**](LimitExpression.md) |  | [optional] 

## Methods

### NewSelectQuery

`func NewSelectQuery(columnClauses []ColumnExpression, ) *SelectQuery`

NewSelectQuery instantiates a new SelectQuery object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewSelectQueryWithDefaults

`func NewSelectQueryWithDefaults() *SelectQuery`

NewSelectQueryWithDefaults instantiates a new SelectQuery object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetColumnClauses

`func (o *SelectQuery) GetColumnClauses() []ColumnExpression`

GetColumnClauses returns the ColumnClauses field if non-nil, zero value otherwise.

### GetColumnClausesOk

`func (o *SelectQuery) GetColumnClausesOk() (*[]ColumnExpression, bool)`

GetColumnClausesOk returns a tuple with the ColumnClauses field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetColumnClauses

`func (o *SelectQuery) SetColumnClauses(v []ColumnExpression)`

SetColumnClauses sets ColumnClauses field to given value.


### GetWhereClause

`func (o *SelectQuery) GetWhereClause() ColumnExpression`

GetWhereClause returns the WhereClause field if non-nil, zero value otherwise.

### GetWhereClauseOk

`func (o *SelectQuery) GetWhereClauseOk() (*ColumnExpression, bool)`

GetWhereClauseOk returns a tuple with the WhereClause field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetWhereClause

`func (o *SelectQuery) SetWhereClause(v ColumnExpression)`

SetWhereClause sets WhereClause field to given value.

### HasWhereClause

`func (o *SelectQuery) HasWhereClause() bool`

HasWhereClause returns a boolean if a field has been set.

### GetOrderByClause

`func (o *SelectQuery) GetOrderByClause() []OrderByExpression`

GetOrderByClause returns the OrderByClause field if non-nil, zero value otherwise.

### GetOrderByClauseOk

`func (o *SelectQuery) GetOrderByClauseOk() (*[]OrderByExpression, bool)`

GetOrderByClauseOk returns a tuple with the OrderByClause field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOrderByClause

`func (o *SelectQuery) SetOrderByClause(v []OrderByExpression)`

SetOrderByClause sets OrderByClause field to given value.

### HasOrderByClause

`func (o *SelectQuery) HasOrderByClause() bool`

HasOrderByClause returns a boolean if a field has been set.

### GetLimitClause

`func (o *SelectQuery) GetLimitClause() LimitExpression`

GetLimitClause returns the LimitClause field if non-nil, zero value otherwise.

### GetLimitClauseOk

`func (o *SelectQuery) GetLimitClauseOk() (*LimitExpression, bool)`

GetLimitClauseOk returns a tuple with the LimitClause field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLimitClause

`func (o *SelectQuery) SetLimitClause(v LimitExpression)`

SetLimitClause sets LimitClause field to given value.

### HasLimitClause

`func (o *SelectQuery) HasLimitClause() bool`

HasLimitClause returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


