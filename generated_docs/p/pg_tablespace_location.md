# pg_tablespace_location

## Location
[src/backend/utils/adt/misc.c:301-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L301-L369)

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
  - [cstring_to_text](../c/cstring_to_text.md)
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

## Simplified Source

```c
Datum pg_tablespace_location(PG_FUNCTION_ARGS) {
    Oid tablespaceOid = PG_GETARG_OID(0);
    char sourcepath[MAXPGPATH];
    char targetpath[MAXPGPATH];

    // Handle invalid OID - use database default tablespace
    if (tablespaceOid == InvalidOid)
        tablespaceOid = MyDatabaseTableSpace;

    // Return empty string for built-in tablespaces
    if (tablespaceOid == DEFAULTTABLESPACE_OID ||
        tablespaceOid == GLOBALTABLESPACE_OID)
        PG_RETURN_TEXT_P(cstring_to_text(""));

    // Build path to tablespace link: pg_tblspc/<oid>
    snprintf(sourcepath, sizeof(sourcepath), "pg_tblspc/%u", tablespaceOid);

    // Check if path is a symbolic link or directory
    struct stat st;
    if (lstat(sourcepath, &st) < 0)
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not stat file \"%s\": %m", sourcepath)));

    // If it's a directory (in-place tablespace), return the path
    if (!S_ISLNK(st.st_mode))
        PG_RETURN_TEXT_P(cstring_to_text(sourcepath));

    // Read the symbolic link target
    int rllen = readlink(sourcepath, targetpath, sizeof(targetpath));
    if (rllen < 0)
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not read symbolic link \"%s\": %m", sourcepath)));

    // Validate link target length and null-terminate
    if (rllen >= sizeof(targetpath))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("symbolic link \"%s\" target is too long", sourcepath)));

    targetpath[rllen] = '\0';
    PG_RETURN_TEXT_P(cstring_to_text(targetpath));
}
```