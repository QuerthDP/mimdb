# Literal

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Value** | Pointer to [**LiteralValue**](LiteralValue.md) |  | [optional] 

## Methods

### NewLiteral

`func NewLiteral() *Literal`

NewLiteral instantiates a new Literal object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewLiteralWithDefaults

`func NewLiteralWithDefaults() *Literal`

NewLiteralWithDefaults instantiates a new Literal object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetValue

`func (o *Literal) GetValue() LiteralValue`

GetValue returns the Value field if non-nil, zero value otherwise.

### GetValueOk

`func (o *Literal) GetValueOk() (*LiteralValue, bool)`

GetValueOk returns a tuple with the Value field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetValue

`func (o *Literal) SetValue(v LiteralValue)`

SetValue sets Value field to given value.

### HasValue

`func (o *Literal) HasValue() bool`

HasValue returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


