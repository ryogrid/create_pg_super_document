# SearchSysCacheAttNum

## Location
src/backend/utils/cache/syscache.c: 544 - 566

## Overview
Searches for an attribute in a relation by attribute number, excluding dropped attributes from the results.

## Definition
```c
HeapTuple SearchSysCacheAttNum(Oid relid, int16 attnum)
```

## Detailed Description
This function is equivalent to SearchSysCache on the ATTNUM cache, but with special handling for dropped attributes. It searches for an attribute within a specified relation using the attribute number as the key. If the attribute is found but marked as dropped (attisdropped = true), the function returns NULL instead of the tuple, effectively treating dropped attributes as non-existent.

This behavior is convenient for callers that want to act as though dropped attributes don't exist in the system, providing a cleaner interface for attribute lookups that should ignore historical artifacts.

## Parameters / Member Variables
- `relid`: The OID of the relation containing the attribute
- `attnum`: The attribute number (column number) to search for

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache2
  - Int16GetDatum
  - ObjectIdGetDatum
  - HeapTupleIsValid
  - GETSTRUCT
  - ReleaseSysCache
- Called from (representative examples):
  - ATExecSetStatistics (src/backend/commands/tablecmds.c:8680)
  - SearchSysCacheCopyAttNum (src/backend/utils/cache/syscache.c:572)

## Notes and Other Information
- Uses the ATTNUM system cache which indexes attributes by relation OID and attribute number
- Properly handles memory management by releasing the cache tuple when a dropped attribute is found
- Returns NULL for both non-existent attributes and dropped attributes, providing consistent behavior
- Part of PostgreSQL's system catalog caching infrastructure for efficient metadata lookups
- Attribute numbers are typically positive integers, with system columns having negative numbers