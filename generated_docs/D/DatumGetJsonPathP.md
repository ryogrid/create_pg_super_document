# DatumGetJsonPathP

## Location
[src/include/utils/jsonpath.h:35-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/jsonpath.h#L35-L40)

## Overview
DatumGetJsonPathP is an inline function that converts a PostgreSQL Datum to a JsonPath pointer by detoasting the datum without creating a copy.

## Definition

```c
static inline JsonPath *
DatumGetJsonPathP(Datum d)
```
## Detailed Description
DatumGetJsonPathP is a type conversion utility function that safely extracts a JsonPath structure from a PostgreSQL Datum. The function uses the PG_DETOAST_DATUM macro to handle the conversion, which will decompress and/or extract the data from PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) storage if necessary. This function returns a pointer to the actual JsonPath structure without making a copy, making it efficient for read-only operations.

The JsonPath structure represents a compiled JSON path expression used for querying and manipulating JSON and JSONB data in PostgreSQL. It contains a varlena header, version/flags information, and the compiled path expression data.

## Parameters / Member Variables
- : The input Datum containing a JsonPath value that may be stored in TOAST format

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM (macro for detoasting PostgreSQL data)
  - [JsonPath](../J/JsonPath.md) (structure type definition)
- Called from (representative examples):
  - [ExecEvalJsonExprPath](../E/ExecEvalJsonExprPath.md) (src/backend/executor/execExprInterp.c:4293)
  - [contain_mutable_functions_walker](../c/contain_mutable_functions_walker.md) (src/backend/optimizer/util/clauses.c:432)
  - [JsonTableInitPlan](../J/JsonTableInitPlan.md) (src/backend/utils/adt/jsonpath_exec.c:4207)
  - PG_GETARG_JSONPATH_P (macro wrapper in src/include/utils/jsonpath.h:46)

## Notes and Other Information
- This is a static inline function defined in the header file for performance
- The function does not make a copy of the data, so the returned pointer should not be modified
- For cases where a modifiable copy is needed, use DatumGetJsonPathPCopy instead
- The PG_GETARG_JSONPATH_P macro is the typical way this function is used in PostgreSQL function implementations
- This function is part of PostgreSQL's JSON path functionality introduced for SQL/JSON standard compliance

## Simplified Source

```c
static inline JsonPath *
DatumGetJsonPathP(Datum d)
{
    // Convert Datum to JsonPath pointer, detoasting if necessary
    return (JsonPath *) PG_DETOAST_DATUM(d);
}
```