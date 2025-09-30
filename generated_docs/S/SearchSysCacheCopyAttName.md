# SearchSysCacheCopyAttName

## Location
[src/backend/utils/cache/syscache.c:504-522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L504-L522)

## Overview
SearchSysCacheCopyAttName is a specialized function that searches for an attribute by relation OID and name, excludes dropped attributes, and returns a copy of the found tuple.

## Definition
```c
HeapTuple SearchSysCacheCopyAttName(Oid relid, const char *attname)
```

## Detailed Description
This function combines the functionality of SearchSysCacheAttName with heap_copytuple to provide a dropped-attribute-aware version of SearchSysCacheCopy for the ATTNAME cache. It first calls SearchSysCacheAttName to locate the attribute tuple while automatically filtering out any dropped attributes. If a valid tuple is found, it creates a copy using heap_copytuple, releases the original cached tuple, and returns the copy.

The function provides callers with a modifiable copy of the attribute tuple that they can safely modify without affecting the cached version. This is particularly useful in DDL operations where attribute metadata needs to be updated. The automatic filtering of dropped attributes means callers don't need to explicitly check the attisdropped flag.

## Parameters / Member Variables
- `relid`: The OID of the relation (table/index/view) containing the attribute
- `attname`: The name of the attribute to search for (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheAttName](SearchSysCacheAttName.md)
  - HeapTupleIsValid
  - [heap_copytuple](../h/heap_copytuple.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [renameatt_internal](../r/renameatt_internal.md)
  - [ATExecAddColumn](../A/ATExecAddColumn.md)
  - [ATExecDropNotNull](../A/ATExecDropNotNull.md)
  - [ATExecSetNotNull](../A/ATExecSetNotNull.md)
  - [ATExecAddIdentity](../A/ATExecAddIdentity.md)
  - [ATExecSetIdentity](../A/ATExecSetIdentity.md)
  - [ATExecDropIdentity](../A/ATExecDropIdentity.md)
  - [ATPrepDropExpression](../A/ATPrepDropExpression.md)
  - [ATExecDropExpression](../A/ATExecDropExpression.md)
  - [ATExecSetStorage](../A/ATExecSetStorage.md)
  - [ATExecDropColumn](../A/ATExecDropColumn.md)
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md)
  - [MergeAttributesIntoExisting](../M/MergeAttributesIntoExisting.md)
  - [ATExecSetCompression](../A/ATExecSetCompression.md)

## Notes and Other Information
- Returns a copy of the attribute tuple that the caller owns and must eventually free with heap_freetuple()
- Returns NULL if the attribute doesn't exist or if it exists but is marked as dropped
- The returned tuple is not connected to the system cache, so modifications to it don't affect the cached version
- Particularly useful in ALTER TABLE operations where attribute definitions need to be modified
- Combines the convenience of automatic dropped-attribute filtering with the safety of tuple copying
- Widely used in DDL operations that modify attribute properties
- The function eliminates the need for callers to manually handle both the dropped-attribute check and tuple copying

## Simplified Source

```c
HeapTuple
SearchSysCacheCopyAttName(Oid relid, const char *attname)
{
    // Search for attribute, filtering out dropped attributes
    HeapTuple tuple = SearchSysCacheAttName(relid, attname);

    // Return NULL if not found
    if (!HeapTupleIsValid(tuple))
        return tuple;

    // Make a copy of the tuple that caller can modify
    HeapTuple newtuple = heap_copytuple(tuple);
    ReleaseSysCache(tuple);

    return newtuple;
}
```