# PLyArrayToOb

## Location
[src/pl/plpython/plpy_typeio.h:35-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.h#L35-L38)

## Overview
PLyArrayToOb is a struct that contains conversion information for transforming PostgreSQL array types to Python list objects.

## Definition


## Detailed Description
PLyArrayToOb handles the conversion of PostgreSQL arrays to Python lists. It uses a recursive approach where it stores a pointer to the conversion information for the array's element type, allowing for nested arrays and complex element types. The conversion process iterates through the PostgreSQL array structure and applies the element conversion function to each item.

## Parameters / Member Variables
- : Pointer to PLyDatumToOb conversion information for the array's element type, enabling recursive conversion of nested structures

## Dependencies
- Functions called/Symbols referenced:
  - [PLyDatumToOb](PLyDatumToOb.md) (for element type conversion)
- Called from (representative examples):
  - [PLyDatumToOb](PLyDatumToOb.md) (as part of the union)
  - [PLyList_FromArray](PLyList_FromArray.md) functions

## Notes and Other Information
This struct enables PostgreSQL's multidimensional arrays to be properly converted to nested Python lists. The recursive nature allows for arrays of any supported PostgreSQL type, including arrays of composite types or other arrays.