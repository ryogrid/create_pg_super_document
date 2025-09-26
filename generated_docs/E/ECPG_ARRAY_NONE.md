# ECPG_ARRAY_NONE

## Location
[src/interfaces/ecpg/ecpglib/ecpglib_extern.h:32-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/ecpglib_extern.h#L32-L34)

## Overview
ECPG_ARRAY_NONE is an enumeration value that indicates a PostgreSQL data type is not treated as an array type within the ECPG (Embedded C PostgreSQL) system, representing scalar data types.

## Definition

```c
struct ECPGgeneric_varchar
{
	int			len;
	char		arr[FLEXIBLE_ARRAY_MEMBER];
};
```
## Detailed Description
ECPG_ARRAY_NONE is the fifth and final enumeration value in the ARRAY_TYPE enum, used to classify PostgreSQL data types that should be treated as scalar (non-array) types in ECPG's type information caching system. This value is assigned to standard scalar data types like BOOLOID, BYTEAOID, CHAROID, INT8OID, INT2OID, INT4OID, and others during the population of ECPG's type cache. The value explicitly indicates that the associated PostgreSQL type is not an array and should be processed as a single scalar value rather than as a collection of elements.

## Parameters / Member Variables
- This is an enumeration constant with no parameters or members
- Value: The exact numeric value is implementation-defined but is the last value in the ARRAY_TYPE enumeration

## Dependencies
- Functions called/Symbols referenced:
  - None (enumeration constant)
- Called from (representative examples):
  -  function in  (type validation)
  -  macro usage throughout  (type cache population)
  -  function in 
  -  function in 
  -  function in 

## Notes and Other Information
- Used extensively in type cache initialization to mark scalar types like BOOL, BYTEA, CHAR, INT8, INT2, INT4, etc.
- The  macro provides semantic clarity in code
- Distinguished from  and  which represent actual array types
- The  macro explicitly excludes , confirming it represents non-array types
- Used in INFORMIX compatibility mode for numeric truncation handling when processing scalar values
- Essential for ECPG's type system to distinguish between scalar and array data types for proper SQL processing and data conversion