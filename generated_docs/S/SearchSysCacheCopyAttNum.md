# SearchSysCacheCopyAttNum

## Location
[src/backend/utils/cache/syscache.c:567-600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L567-L600)

## Overview
Returns a copy of an attribute tuple from the system cache, excluding dropped attributes from the results.

## Definition
```c
HeapTuple SearchSysCacheCopyAttNum(Oid relid, int16 attnum)
```

## Detailed Description
This function is an attisdropped-aware version of SearchSysCacheCopy that provides a copy of an attribute tuple rather than a reference to the cached version. It internally uses SearchSysCacheAttNum to locate the attribute, which automatically excludes dropped attributes. If a valid attribute is found, the function creates a copy of the tuple using heap_copytuple and properly releases the original cached tuple.

The returned copy is independent of the system cache and can be modified or retained without affecting the cache or other concurrent operations. This makes it suitable for situations where the caller needs to modify the tuple data or keep it beyond the normal cache lifetime.

## Parameters / Member Variables
- `relid`: The OID of the relation containing the attribute
- `attnum`: The attribute number to search for and copy

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheAttNum](SearchSysCacheAttNum.md)
  - HeapTupleIsValid
  - [heap_copytuple](../h/heap_copytuple.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [SetIndexStorageProperties](SetIndexStorageProperties.md) (src/backend/commands/tablecmds.c:8856)

## Notes and Other Information
- Returns a newly allocated tuple that the caller is responsible for freeing
- Provides safe access to attribute information without cache lifetime constraints
- Automatically excludes dropped attributes through its use of SearchSysCacheAttNum
- The returned tuple is a complete copy, including all attribute data and metadata
- Memory management is handled properly - the original cached tuple is released after copying
- Part of PostgreSQL's system cache infrastructure optimized for scenarios requiring tuple modification or extended retention