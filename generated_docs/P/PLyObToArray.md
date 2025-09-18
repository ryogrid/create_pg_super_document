# PLyObToArray

## Location
[src/pl/plpython/plpy_typeio.h:99-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.h#L99-L103)

## Overview
PLyObToArray is a specialized conversion structure used within PostgreSQL's PLpython extension to handle conversion of Python sequences to PostgreSQL array data types.

## Definition
```c
typedef struct PLyObToArray
{
    PLyObToDatum *elm;          /* conversion info for array's element type */
    Oid          elmbasetype;   /* element base type */
} PLyObToArray;
```

## Detailed Description
PLyObToArray is a component structure used as part of the PLyObToDatum conversion system specifically for handling PostgreSQL array types. It provides the necessary information to convert Python sequences (lists, tuples, etc.) into PostgreSQL arrays by maintaining conversion information for the array's element type and tracking the base type of those elements. This structure enables recursive conversion of array elements while preserving PostgreSQL's array type semantics.

## Parameters / Member Variables
- `elm`: Pointer to PLyObToDatum structure containing conversion information for the array's element type, enabling recursive conversion of individual array elements
- `elmbasetype`: OID of the array element's base type, used for proper array construction and type validation

## Dependencies
- Functions called/Symbols referenced:
  - [PLyObToDatum](PLyObToDatum.md) (for element conversion)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [PLyObToDatum](PLyObToDatum.md) (as union member 'array')
  - [PLySequence_ToArray](PLySequence_ToArray.md) functions in plpy_typeio.c
  - Array conversion functions in PLpython

## Notes and Other Information
PLyObToArray enables PostgreSQL's PLpython extension to handle multi-dimensional arrays and nested array structures by providing recursive element conversion capabilities. The elm field points to another PLyObToDatum structure, which may itself contain array conversion information for multi-dimensional arrays. The elmbasetype is essential for PostgreSQL's array construction functions that need to know the base type for proper memory allocation and type checking. This structure works in conjunction with PostgreSQL's array building functions to construct proper PostgreSQL array values from Python sequences while maintaining type safety and proper array semantics.