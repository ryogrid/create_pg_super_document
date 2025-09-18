# buildint2vector

## Location
src/backend/utils/adt/int.c: 114 - 140

## Overview
The buildint2vector function constructs an int2vector data structure from a raw array of int16 values, setting up the proper PostgreSQL array metadata.

## Definition
```c
int2vector *buildint2vector(const int16 *int2s, int n)
```

## Detailed Description
buildint2vector is a utility function that constructs a PostgreSQL int2vector from an array of int16 values. An int2vector is a specialized PostgreSQL array type used primarily for storing lists of integers in system catalogs, such as index key column numbers. The function allocates memory for the int2vector structure, copies the provided int16 values (if any), and sets up the standard PostgreSQL array header with appropriate metadata. The function supports creating empty vectors when int2s is NULL, allowing the caller to fill in values afterward. The resulting int2vector follows PostgreSQL's array conventions with a lower bound of 0 rather than 1 for historical reasons.

## Parameters / Member Variables
- `int2s`: Pointer to a raw array of int16 values to copy into the vector (can be NULL)
- `n`: Number of elements in the array

## Dependencies
- Functions called/Symbols referenced:
  - [int2vector](../i/int2vector.md) (type)
  - Int2VectorSize
  - SET_VARSIZE
- Called from (representative examples):
  - [StorePartitionKey](../S/StorePartitionKey.md)
  - UpdateIndexRelation
  - [publication_add_relation](../p/publication_add_relation.md)
  - [CreateStatistics](../C/CreateStatistics.md)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md)

## Notes and Other Information
- Used primarily in PostgreSQL system catalog operations
- Creates vectors with lower bound 0 instead of 1 for historical compatibility
- Memory is allocated using palloc0 which zero-initializes the structure
- Sets up complete array metadata including dimensions, element type (INT2OID), and data offset
- The dataoffset is set to 0 since int2vectors never contain null values
- If int2s is NULL, the caller is responsible for filling the values array afterward
- Memory allocation size is calculated using Int2VectorSize macro
- Returns a fully initialized int2vector ready for use in PostgreSQL internals