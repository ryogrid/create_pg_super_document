# AttrDefaultFetch

## Location
src/backend/utils/cache/relcache.c: 4490 - 4569

## Overview
Loads attribute default value definitions from the pg_attrdef catalog for a relation, building an array of AttrDefault entries sorted by attribute number.

## Definition
```c
static void AttrDefaultFetch(Relation relation, int ndef)
```

## Detailed Description
AttrDefaultFetch retrieves and processes default value expressions for table columns that have been marked with atthasdef. The function performs a systematic scan of the pg_attrdef system catalog to find default expressions corresponding to the specified relation.

The function allocates memory in CacheMemoryContext for an array of AttrDefault structures, then scans pg_attrdef using the relation's OID as the search key. For each matching record, it extracts the default expression (adbin field), converts it from internal format to a C string, and stores it along with the attribute number in the AttrDefault array.

The implementation includes robust error handling for missing or unexpected records, issuing warnings rather than hard errors to allow PostgreSQL to continue operating even with incomplete default value information. After loading all records, the function sorts the array by attribute number for efficient lookup and installs it in the relation's tuple descriptor constraint structure.

## Parameters
- `relation`: The Relation structure for which to fetch default values
- `ndef`: Expected number of attributes that have default values (those marked with atthasdef)

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAllocZero
  - ScanKeyInit
  - table_open
  - systable_beginscan
  - systable_getnext
  - fastgetattr
  - TextDatumGetCString
  - MemoryContextStrdup
  - qsort
  - AttrDefaultCmp
- Called from:
  - RelationBuildTupleDesc

## Notes and Other Information
- Memory allocation occurs in CacheMemoryContext to ensure persistence
- Handles gracefully missing pg_attrdef records by issuing warnings rather than errors
- Sorts AttrDefault entries by attribute number for efficient access by equalTupleDescs()
- Detoasts and converts default expressions from Datum to C string format
- Updates relation's constraint structure with the loaded default values
- Part of the relation cache building process during tuple descriptor construction