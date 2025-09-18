# opfamily_can_sort_type

## Location
src/backend/access/index/amvalidate.c: 271 - 276

## Overview
Determines whether a specific data type is a legitimate input type for a btree operator family, effectively checking if the data type can be sorted using that operator family.

## Definition
```c
bool opfamily_can_sort_type(Oid opfamilyoid, Oid datatypeoid)
```

## Detailed Description
This function serves as a utility to validate whether a given data type is compatible with a btree operator family for sorting operations. It acts as a wrapper around `opclass_for_family_datatype`, checking if there exists a valid operator class within the specified btree operator family that can handle the given data type. The function is primarily used in access method validation contexts to ensure that operator families are properly configured to support the data types they claim to handle.

The function specifically targets btree access methods (BTREE_AM_OID) and returns true only if a valid operator class OID is found, indicating that the data type can indeed be sorted using the operators defined in the family.

## Parameters / Member Variables
- `opfamilyoid`: The OID of the btree operator family to check against
- `datatypeoid`: The OID of the data type to validate for sorting compatibility

## Dependencies
- Functions called/Symbols referenced:
  - [opclass_for_family_datatype](opclass_for_family_datatype.md)
  - OidIsValid
  - BTREE_AM_OID (constant)
- Called from (representative examples):
  - [gistvalidate](../g/gistvalidate.md)
  - [spgproperty](../s/spgproperty.md)  
  - [spgvalidate](../s/spgvalidate.md)

## Notes and Other Information
- This function is located in src/backend/access/index/amvalidate.c:271-276
- The function is specifically designed for btree operator families and uses the hardcoded BTREE_AM_OID constant
- It relies on the related function `opclass_for_family_datatype` which finds the OID of an operator class that belongs to a specified operator family and accepts a given data type as input
- The function is used in validation contexts across different access methods (GiST, SP-GiST) to ensure proper operator family configuration
- Returns a boolean value: true if the data type can be sorted by the operator family, false otherwise