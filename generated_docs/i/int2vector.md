# int2vector

## Location
[src/include/c.h:723-733](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/c.h#L723-L733)

## Overview
A specialized data structure representing a variable-length array of 16-bit signed integers (int16), designed to efficiently store vectors of small integer values in PostgreSQL's catalog system.

## Definition
```c
typedef struct
{
    int32       vl_len_;        /* these fields must match ArrayType! */
    int         ndim;           /* always 1 for int2vector */
    int32       dataoffset;     /* always 0 for int2vector */
    Oid         elemtype;
    int         dim1;
    int         lbound1;
    int16       values[FLEXIBLE_ARRAY_MEMBER];
} int2vector;
```

## Detailed Description
The int2vector structure is a specialized array type used primarily in PostgreSQL's system catalogs to store vectors of 16-bit integers. It follows the same memory layout as PostgreSQL's general ArrayType structure but is optimized for single-dimensional arrays of int16 values. This structure is commonly used to store index column information, trigger attributes, and other catalog metadata that requires compact integer arrays. The structure uses PostgreSQL's variable-length (varlena) format, making it suitable for storage in catalog tables.

## Parameters / Member Variables
- `vl_len_`: Variable-length header containing the total size of the structure in bytes
- `ndim`: Number of dimensions (always 1 for int2vector, indicating a single-dimensional array)
- `dataoffset`: Offset to actual data (always 0 for int2vector, indicating no null bitmap)
- `elemtype`: OID of the element type (typically INT2OID for int16 elements)
- `dim1`: Number of elements in the first (and only) dimension
- `lbound1`: Lower bound of the first dimension (typically 1)
- `values`: Flexible array member containing the actual int16 values

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (for variable-length array declaration)
- Called from (representative examples):
  - [buildint2vector](../b/buildint2vector.md) (constructs int2vector instances)
  - [int2vectorin](int2vectorin.md)/int2vectorout (input/output functions)
  - Int2VectorSize (calculates required size)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md) (used in index definition formatting)
  - Various catalog operations in pg_index, pg_trigger, and other system catalogs

## Notes and Other Information
- This structure must maintain compatibility with ArrayType for proper integration with PostgreSQL's array handling system
- Primarily used in system catalogs for storing compact integer arrays such as index column numbers and trigger attributes
- The flexible array member allows for efficient storage of variable-length integer vectors
- Memory layout is optimized for direct storage in PostgreSQL's heap pages
- Input/output functions handle conversion between text representation and binary format
- Size calculation functions ensure proper memory allocation for variable-length instances