# pg_tablespace_location

## Location
src/backend/utils/adt/misc.c: 301 - 369

## Overview
Returns the filesystem path where a tablespace is located by resolving symbolic links in the pg_tblspc directory.

## Definition
```c
Datum pg_tablespace_location(PG_FUNCTION_ARGS)
```

## Detailed Description
This function takes a tablespace OID and returns the actual filesystem path where the tablespace data is stored. The function handles several cases:

1. **Invalid OID (0)**: Treats it as the current database's default tablespace (MyDatabaseTableSpace)
2. **Built-in tablespaces** (DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID): Returns an empty string since these don't have explicit locations
3. **User-defined tablespaces**: Resolves the symbolic link in "pg_tblspc/{oid}" to get the target path

The function supports both symbolic links and directories:
- For symbolic links: Reads the link target using readlink() and returns the resolved path
- For directories: Returns the relative path (for in-place tablespaces created with allow_in_place_tablespaces)

Error handling includes file access failures, link resolution failures, and path length validation.

## Parameters / Member Variables
- `tablespaceOid`: The OID of the tablespace whose location to retrieve (retrieved via PG_GETARG_OID(0))

## Dependencies
- Functions called/Symbols referenced:
  - lstat
  - readlink  
  - cstring_to_text
  - PG_RETURN_TEXT_P
  - S_ISLNK
  - snprintf
  - ereport/errmsg/errcode
- Constants referenced:
  - InvalidOid
  - MyDatabaseTableSpace
  - DEFAULTTABLESPACE_OID
  - GLOBALTABLESPACE_OID
  - MAXPGPATH
- Called from:
  - SQL function calls (no direct C references found)

## Notes and Other Information
- This function is commonly used in system catalog queries and administrative tools to determine where tablespace data is physically stored
- The function gracefully handles the case where OID 0 represents the default tablespace
- Built-in tablespaces return empty strings because their locations are implicit (data directory)
- The function validates symbolic link target length to prevent buffer overflows
- Supports the allow_in_place_tablespaces feature by handling directories in addition to symbolic links
- Returns text data type that can be used directly in SQL queries