# gin_extract_jsonb_path

## Location
src/backend/utils/adt/jsonb_gin.c: 1090 - 1179

## Overview
The value extraction function for the jsonb_path_ops GIN opclass that generates hash-based index entries incorporating both JSON values and their hierarchical key paths.

## Definition
```c
Datum gin_extract_jsonb_path(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the jsonb_path_ops GIN extraction method, which creates a more selective index than the standard jsonb_ops by including the full key path in hash computations. Each index entry is a uint32 hash that combines both the JSON value and all the keys leading to that value in the hierarchy. This approach enables the index to distinguish between structurally different but value-equivalent JSON, such as {"foo": 42} versus {"bar": 42}.

The function uses a stack-based approach to track the hierarchical path through nested JSON objects and arrays. As it traverses the JSON structure using JsonbIterator, it maintains cumulative hashes that incorporate parent keys, ensuring that each final hash represents both the value and its complete path context.

## Parameters / Member Variables
- `jb`: Input JSONB value to extract index entries from
- `nentries`: Output parameter for the number of extracted entries

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - PG_GETARG_POINTER
  - JB_ROOT_COUNT
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [JsonbHashScalarValue](../J/JsonbHashScalarValue.md)
  - [init_gin_entries](../i/init_gin_entries.md)
  - [add_gin_entry](../a/add_gin_entry.md)
  - [UInt32GetDatum](../U/UInt32GetDatum.md)
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - elog
  - PG_RETURN_POINTER
- Types and constants:
  - JsonbIterator
  - [JsonbValue](../J/JsonbValue.md)
  - JsonbIteratorToken
  - PathHashStack
  - GinEntries
  - WJB_DONE, WJB_BEGIN_ARRAY, WJB_BEGIN_OBJECT
  - WJB_KEY, WJB_ELEM, WJB_VALUE
  - WJB_END_ARRAY, WJB_END_OBJECT
- Called from:
  - [gin_extract_jsonb_query_path](gin_extract_jsonb_query_path.md) (for query processing)

## Notes and Other Information
- Specifically designed for jsonb_path_ops opclass, not the standard jsonb_ops
- Only supports containment queries (@>) due to path-sensitive hashing
- Provides better selectivity than jsonb_ops by distinguishing structural differences
- Uses stack-based hash computation to include full key paths in index entries
- Hash values incorporate both the JSON value and all parent keys in the hierarchy
- More efficient for containment queries but supports fewer query types than jsonb_ops
- Located in src/backend/utils/adt/jsonb_gin.c:1090-1179