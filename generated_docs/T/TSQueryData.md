# TSQueryData

## Location
src/include/tsearch/ts_type.h: 225 - 226

## Overview
TSQueryData is a PostgreSQL data structure that represents the internal storage format for text search queries (tsquery). It serves as the base structure for TSQuery objects and contains the actual query data in a variable-length format.

## Definition
```c
typedef struct
{
    int32       vl_len_;        /* varlena header (do not touch directly!) */
    int32       size;           /* number of QueryItems */
    char        data[FLEXIBLE_ARRAY_MEMBER];    /* data starts here */
} TSQueryData;
```

## Detailed Description
TSQueryData implements the storage format for PostgreSQL text search queries. It follows PostgreSQL's varlena (variable-length) storage convention, allowing it to store queries of arbitrary complexity. The structure contains a header followed by an array of QueryItem objects and the actual string operands.

The storage layout is organized as:
1. `vl_len_` - Standard PostgreSQL varlena header for variable-length objects
2. `size` - Count of QueryItem objects in the query
3. `data[]` - Flexible array containing:
   - Array of QueryItem unions (operators and operands)
   - Null-terminated C strings for the actual search terms

This design enables efficient storage and processing of complex text search queries including boolean operators (AND, OR, NOT), phrase searches, and weighted searches.

## Parameters / Member Variables
- `vl_len_`: PostgreSQL varlena header that stores the total length of the structure; managed automatically by the varlena system and should not be accessed directly
- `size`: The number of QueryItem objects contained in this query structure
- `data[]`: Flexible array member that contains the actual query data - both the QueryItem array and the operand strings stored contiguously

## Dependencies
- Functions called/Symbols referenced:
  - Uses QueryItem (union type for query operators and operands)
  - Uses QueryItemType (int8 type for item classification)
  - Follows PostgreSQL varlena conventions

- Called from (representative examples):
  - TSQuery (typedef pointer to TSQueryData)
  - [TSQueryGetDatum](TSQueryGetDatum.md) (conversion function)

## Notes and Other Information
- [TSQueryData](TSQueryData.md) uses PostgreSQL's flexible array member pattern for efficient memory layout
- The structure is 4-byte aligned to ensure proper access to QueryItem elements
- Total size can be computed using the COMPUTESIZE macro: HDRSIZETQ + (size * sizeof(QueryItem)) + length_of_operands
- Access macros are provided: GETQUERY(x) for QueryItem array, GETOPERAND(x) for operand strings
- The structure supports complex query expressions with boolean operators, phrase searches, and term weighting
- Memory layout is optimized for both storage efficiency and query execution performance