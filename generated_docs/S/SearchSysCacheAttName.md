# SearchSysCacheAttName

## Location
src/backend/utils/cache/syscache.c: 481 - 503

## Overview
SearchSysCacheAttName is a specialized function that searches for an attribute by relation OID and attribute name, but excludes dropped attributes from the results.

## Definition
```c
HeapTuple SearchSysCacheAttName(Oid relid, const char *attname)
```

## Detailed Description
This function provides a specialized interface to the ATTNAME system cache that automatically filters out dropped attributes. It performs a search for an attribute tuple using the relation OID and attribute name as keys, but unlike a direct SearchSysCache2 call, it checks the attisdropped flag of the found tuple and returns NULL if the attribute has been dropped. This behavior is convenient for callers that want to act as though dropped attributes don't exist in the system.

The function first searches the ATTNAME cache using SearchSysCache2 with the provided relation OID and attribute name. If a tuple is found, it checks the attisdropped field of the pg_attribute structure. If the attribute is marked as dropped, the function releases the cache entry and returns NULL. Otherwise, it returns the tuple with a reference that the caller must eventually release.

## Parameters / Member Variables
- `relid`: The OID of the relation (table/index/view) containing the attribute
- `attname`: The name of the attribute to search for (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache2
  - ObjectIdGetDatum
  - CStringGetDatum
  - HeapTupleIsValid
  - GETSTRUCT
  - ReleaseSysCache
  - Form_pg_attribute (type cast)
- Called from (representative examples):
  - pg_nextoid
  - SetAttrMissing
  - ComputeIndexAttrs
  - CreateStatistics
  - ATPrepSetNotNull
  - ATExecCheckNotNull
  - ATExecSetExpression
  - ATExecSetStatistics
  - ATExecSetOptions
  - ATExecDropColumn
  - transformColumnNameList
  - ATPrepAlterColumnType
  - ATExecAlterColumnGenericOptions
  - ComputePartitionAttrs
  - make_inh_translation_list
  - get_attnum
  - SearchSysCacheCopyAttName
  - SearchSysCacheExistsAttName

## Notes and Other Information
- Specifically designed for the ATTNAME cache, which indexes pg_attribute tuples by (relation OID, attribute name)
- The key difference from SearchSysCache2(ATTNAME, ...) is the automatic filtering of dropped attributes
- Dropped attributes in PostgreSQL are not physically removed immediately but are marked with attisdropped=true
- Returns NULL if the attribute doesn't exist or if it exists but is marked as dropped
- Callers must call ReleaseSysCache() on the returned tuple if it's not NULL
- Widely used in DDL operations, query planning, and attribute resolution where dropped attributes should be ignored
- The function simplifies code by avoiding the need for callers to manually check the attisdropped flag