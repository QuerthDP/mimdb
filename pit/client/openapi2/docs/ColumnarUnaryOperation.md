# ColumnarUnaryOperation

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Operand** | Pointer to [**ColumnExpression**](ColumnExpression.md) |  | [optional] 
**Operator** | Pointer to **string** |  | [optional] 

## Methods

### NewColumnarUnaryOperation

`func NewColumnarUnaryOperation() *ColumnarUnaryOperation`

NewColumnarUnaryOperation instantiates a new ColumnarUnaryOperation object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewColumnarUnaryOperationWithDefaults

`func NewColumnarUnaryOperationWithDefaults() *ColumnarUnaryOperation`

NewColumnarUnaryOperationWithDefaults instantiates a new ColumnarUnaryOperation object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetOperand

`func (o *ColumnarUnaryOperation) GetOperand() ColumnExpression`

GetOperand returns the Operand field if non-nil, zero value otherwise.

### GetOperandOk

`func (o *ColumnarUnaryOperation) GetOperandOk() (*ColumnExpression, bool)`

GetOperandOk returns a tuple with the Operand field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOperand

`func (o *ColumnarUnaryOperation) SetOperand(v ColumnExpression)`

SetOperand sets Operand field to given value.

### HasOperand

`func (o *ColumnarUnaryOperation) HasOperand() bool`

HasOperand returns a boolean if a field has been set.

### GetOperator

`func (o *ColumnarUnaryOperation) GetOperator() string`

GetOperator returns the Operator field if non-nil, zero value otherwise.

### GetOperatorOk

`func (o *ColumnarUnaryOperation) GetOperatorOk() (*string, bool)`

GetOperatorOk returns a tuple with the Operator field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOperator

`func (o *ColumnarUnaryOperation) SetOperator(v string)`

SetOperator sets Operator field to given value.

### HasOperator

`func (o *ColumnarUnaryOperation) HasOperator() bool`

HasOperator returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


