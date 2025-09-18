# FindDefaultConversion

## Location
[src/backend/catalog/pg_conversion.c:152-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_conversion.c#L152-L177)

## Overview
Searches for and returns the procedure OID of a default encoding conversion for a specific encoding pair within a given namespace.

## Definition
```c
Oid FindDefaultConversion(Oid name_space, int32 for_encoding, int32 to_encoding)
```

## Detailed Description
FindDefaultConversion locates the default conversion procedure for a specific source and target encoding pair within a specified namespace. It searches the pg_conversion system catalog using a cache list lookup and iterates through matching entries to find one marked as default (condefault=true). The function is essential for PostgreSQL's automatic encoding conversion system, allowing the database to automatically apply appropriate conversions when data needs to be converted between different character encodings. Note that the function returns the conversion procedure's OID, not the conversion catalog entry's OID.

## Parameters / Member Variables
- `name_space`: OID of the namespace to search within
- `for_encoding`: Source encoding ID to convert from
- `to_encoding`: Target encoding ID to convert to

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheList3
  - ReleaseSysCacheList
  - Form_pg_conversion (struct type)
  - CatCList (struct type)
- Called from (representative examples):
  - [FindDefaultConversionProc](FindDefaultConversionProc.md)
  - [ConversionCreate](../C/ConversionCreate.md)

## Notes and Other Information
- Returns InvalidOid if no default conversion is found for the specified encoding pair
- Returns the procedure OID (conproc field) rather than the conversion OID
- Uses the CONDEFAULT system cache for efficient lookup by namespace and encoding pair
- Only considers conversions marked as default (condefault=true)
- Properly releases the system cache list to prevent memory leaks
- Critical for PostgreSQL's automatic character set conversion functionality