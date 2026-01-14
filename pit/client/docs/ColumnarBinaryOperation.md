# ColumnarBinaryOperation

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Operator** | Pointer to **string** |  | [optional] 
**LeftOperand** | Pointer to [**ColumnExpression**](ColumnExpression.md) |  | [optional] 
**RightOperand** | Pointer to [**ColumnExpression**](ColumnExpression.md) |  | [optional] 

## Methods

### NewColumnarBinaryOperation

`func NewColumnarBinaryOperation() *ColumnarBinaryOperation`

NewColumnarBinaryOperation instantiates a new ColumnarBinaryOperation object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewColumnarBinaryOperationWithDefaults

`func NewColumnarBinaryOperationWithDefaults() *ColumnarBinaryOperation`

NewColumnarBinaryOperationWithDefaults instantiates a new ColumnarBinaryOperation object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetOperator

`func (o *ColumnarBinaryOperation) GetOperator() string`

GetOperator returns the Operator field if non-nil, zero value otherwise.

### GetOperatorOk

`func (o *ColumnarBinaryOperation) GetOperatorOk() (*string, bool)`

GetOperatorOk returns a tuple with the Operator field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOperator

`func (o *ColumnarBinaryOperation) SetOperator(v string)`

SetOperator sets Operator field to given value.

### HasOperator

`func (o *ColumnarBinaryOperation) HasOperator() bool`

HasOperator returns a boolean if a field has been set.

### GetLeftOperand

`func (o *ColumnarBinaryOperation) GetLeftOperand() ColumnExpression`

GetLeftOperand returns the LeftOperand field if non-nil, zero value otherwise.

### GetLeftOperandOk

`func (o *ColumnarBinaryOperation) GetLeftOperandOk() (*ColumnExpression, bool)`

GetLeftOperandOk returns a tuple with the LeftOperand field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLeftOperand

`func (o *ColumnarBinaryOperation) SetLeftOperand(v ColumnExpression)`

SetLeftOperand sets LeftOperand field to given value.

### HasLeftOperand

`func (o *ColumnarBinaryOperation) HasLeftOperand() bool`

HasLeftOperand returns a boolean if a field has been set.

### GetRightOperand

`func (o *ColumnarBinaryOperation) GetRightOperand() ColumnExpression`

GetRightOperand returns the RightOperand field if non-nil, zero value otherwise.

### GetRightOperandOk

`func (o *ColumnarBinaryOperation) GetRightOperandOk() (*ColumnExpression, bool)`

GetRightOperandOk returns a tuple with the RightOperand field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRightOperand

`func (o *ColumnarBinaryOperation) SetRightOperand(v ColumnExpression)`

SetRightOperand sets RightOperand field to given value.

### HasRightOperand

`func (o *ColumnarBinaryOperation) HasRightOperand() bool`

HasRightOperand returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


