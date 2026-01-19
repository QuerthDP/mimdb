# SystemInformation

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**InterfaceVersion** | Pointer to **string** | Version of the DBMS interface | [optional] 
**Version** | **string** | Version of the DBMS system | 
**Author** | **string** | Author of the DBMS system (will help me to automate testing) | 
**Uptime** | Pointer to **int64** | System uptime in seconds | [optional] 

## Methods

### NewSystemInformation

`func NewSystemInformation(version string, author string, ) *SystemInformation`

NewSystemInformation instantiates a new SystemInformation object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewSystemInformationWithDefaults

`func NewSystemInformationWithDefaults() *SystemInformation`

NewSystemInformationWithDefaults instantiates a new SystemInformation object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetInterfaceVersion

`func (o *SystemInformation) GetInterfaceVersion() string`

GetInterfaceVersion returns the InterfaceVersion field if non-nil, zero value otherwise.

### GetInterfaceVersionOk

`func (o *SystemInformation) GetInterfaceVersionOk() (*string, bool)`

GetInterfaceVersionOk returns a tuple with the InterfaceVersion field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetInterfaceVersion

`func (o *SystemInformation) SetInterfaceVersion(v string)`

SetInterfaceVersion sets InterfaceVersion field to given value.

### HasInterfaceVersion

`func (o *SystemInformation) HasInterfaceVersion() bool`

HasInterfaceVersion returns a boolean if a field has been set.

### GetVersion

`func (o *SystemInformation) GetVersion() string`

GetVersion returns the Version field if non-nil, zero value otherwise.

### GetVersionOk

`func (o *SystemInformation) GetVersionOk() (*string, bool)`

GetVersionOk returns a tuple with the Version field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersion

`func (o *SystemInformation) SetVersion(v string)`

SetVersion sets Version field to given value.


### GetAuthor

`func (o *SystemInformation) GetAuthor() string`

GetAuthor returns the Author field if non-nil, zero value otherwise.

### GetAuthorOk

`func (o *SystemInformation) GetAuthorOk() (*string, bool)`

GetAuthorOk returns a tuple with the Author field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAuthor

`func (o *SystemInformation) SetAuthor(v string)`

SetAuthor sets Author field to given value.


### GetUptime

`func (o *SystemInformation) GetUptime() int64`

GetUptime returns the Uptime field if non-nil, zero value otherwise.

### GetUptimeOk

`func (o *SystemInformation) GetUptimeOk() (*int64, bool)`

GetUptimeOk returns a tuple with the Uptime field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUptime

`func (o *SystemInformation) SetUptime(v int64)`

SetUptime sets Uptime field to given value.

### HasUptime

`func (o *SystemInformation) HasUptime() bool`

HasUptime returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


