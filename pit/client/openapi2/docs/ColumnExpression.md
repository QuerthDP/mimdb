# ColumnExpression

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**TableName** | Pointer to **string** |  | [optional] 
**ColumnName** | Pointer to **string** |  | [optional] 
**Value** | Pointer to [**LiteralValue**](LiteralValue.md) |  | [optional] 
**FunctionName** | Pointer to **string** |  | [optional] 
**Arguments** | Pointer to [**[]ColumnExpression**](ColumnExpression.md) |  | [optional] 
**Operator** | Pointer to **string** |  | [optional] 
**LeftOperand** | Pointer to [**ColumnExpression**](ColumnExpression.md) |  | [optional] 
**RightOperand** | Pointer to [**ColumnExpression**](ColumnExpression.md) |  | [optional] 
**Operand** | Pointer to [**ColumnExpression**](ColumnExpression.md) |  | [optional] 

## Methods

### NewColumnExpression

`func NewColumnExpression() *ColumnExpression`

NewColumnExpression instantiates a new ColumnExpression object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewColumnExpressionWithDefaults

`func NewColumnExpressionWithDefaults() *ColumnExpression`

NewColumnExpressionWithDefaults instantiates a new ColumnExpression object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetTableName

`func (o *ColumnExpression) GetTableName() string`

GetTableName returns the TableName field if non-nil, zero value otherwise.

### GetTableNameOk

`func (o *ColumnExpression) GetTableNameOk() (*string, bool)`

GetTableNameOk returns a tuple with the TableName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTableName

`func (o *ColumnExpression) SetTableName(v string)`

SetTableName sets TableName field to given value.

### HasTableName

`func (o *ColumnExpression) HasTableName() bool`

HasTableName returns a boolean if a field has been set.

### GetColumnName

`func (o *ColumnExpression) GetColumnName() string`

GetColumnName returns the ColumnName field if non-nil, zero value otherwise.

### GetColumnNameOk

`func (o *ColumnExpression) GetColumnNameOk() (*string, bool)`

GetColumnNameOk returns a tuple with the ColumnName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetColumnName

`func (o *ColumnExpression) SetColumnName(v string)`

SetColumnName sets ColumnName field to given value.

### HasColumnName

`func (o *ColumnExpression) HasColumnName() bool`

HasColumnName returns a boolean if a field has been set.

### GetValue

`func (o *ColumnExpression) GetValue() LiteralValue`

GetValue returns the Value field if non-nil, zero value otherwise.

### GetValueOk

`func (o *ColumnExpression) GetValueOk() (*LiteralValue, bool)`

GetValueOk returns a tuple with the Value field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetValue

`func (o *ColumnExpression) SetValue(v LiteralValue)`

SetValue sets Value field to given value.

### HasValue

`func (o *ColumnExpression) HasValue() bool`

HasValue returns a boolean if a field has been set.

### GetFunctionName

`func (o *ColumnExpression) GetFunctionName() string`

GetFunctionName returns the FunctionName field if non-nil, zero value otherwise.

### GetFunctionNameOk

`func (o *ColumnExpression) GetFunctionNameOk() (*string, bool)`

GetFunctionNameOk returns a tuple with the FunctionName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFunctionName

`func (o *ColumnExpression) SetFunctionName(v string)`

SetFunctionName sets FunctionName field to given value.

### HasFunctionName

`func (o *ColumnExpression) HasFunctionName() bool`

HasFunctionName returns a boolean if a field has been set.

### GetArguments

`func (o *ColumnExpression) GetArguments() []ColumnExpression`

GetArguments returns the Arguments field if non-nil, zero value otherwise.

### GetArgumentsOk

`func (o *ColumnExpression) GetArgumentsOk() (*[]ColumnExpression, bool)`

GetArgumentsOk returns a tuple with the Arguments field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetArguments

`func (o *ColumnExpression) SetArguments(v []ColumnExpression)`

SetArguments sets Arguments field to given value.

### HasArguments

`func (o *ColumnExpression) HasArguments() bool`

HasArguments returns a boolean if a field has been set.

### GetOperator

`func (o *ColumnExpression) GetOperator() string`

GetOperator returns the Operator field if non-nil, zero value otherwise.

### GetOperatorOk

`func (o *ColumnExpression) GetOperatorOk() (*string, bool)`

GetOperatorOk returns a tuple with the Operator field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOperator

`func (o *ColumnExpression) SetOperator(v string)`

SetOperator sets Operator field to given value.

### HasOperator

`func (o *ColumnExpression) HasOperator() bool`

HasOperator returns a boolean if a field has been set.

### GetLeftOperand

`func (o *ColumnExpression) GetLeftOperand() ColumnExpression`

GetLeftOperand returns the LeftOperand field if non-nil, zero value otherwise.

### GetLeftOperandOk

`func (o *ColumnExpression) GetLeftOperandOk() (*ColumnExpression, bool)`

GetLeftOperandOk returns a tuple with the LeftOperand field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLeftOperand

`func (o *ColumnExpression) SetLeftOperand(v ColumnExpression)`

SetLeftOperand sets LeftOperand field to given value.

### HasLeftOperand

`func (o *ColumnExpression) HasLeftOperand() bool`

HasLeftOperand returns a boolean if a field has been set.

### GetRightOperand

`func (o *ColumnExpression) GetRightOperand() ColumnExpression`

GetRightOperand returns the RightOperand field if non-nil, zero value otherwise.

### GetRightOperandOk

`func (o *ColumnExpression) GetRightOperandOk() (*ColumnExpression, bool)`

GetRightOperandOk returns a tuple with the RightOperand field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRightOperand

`func (o *ColumnExpression) SetRightOperand(v ColumnExpression)`

SetRightOperand sets RightOperand field to given value.

### HasRightOperand

`func (o *ColumnExpression) HasRightOperand() bool`

HasRightOperand returns a boolean if a field has been set.

### GetOperand

`func (o *ColumnExpression) GetOperand() ColumnExpression`

GetOperand returns the Operand field if non-nil, zero value otherwise.

### GetOperandOk

`func (o *ColumnExpression) GetOperandOk() (*ColumnExpression, bool)`

GetOperandOk returns a tuple with the Operand field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOperand

`func (o *ColumnExpression) SetOperand(v ColumnExpression)`

SetOperand sets Operand field to given value.

### HasOperand

`func (o *ColumnExpression) HasOperand() bool`

HasOperand returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


