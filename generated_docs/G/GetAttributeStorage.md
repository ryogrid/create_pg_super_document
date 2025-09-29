# GetAttributeStorage

## Location
[src/backend/commands/tablecmds.c:20268-20299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L20268-L20299)

## Overview
Resolves a column storage specification string to a storage type character, validating that the data type supports the requested storage mode.

## Definition
```c
static char GetAttributeStorage(Oid atttypid, const char *storagemode)
```

## Detailed Description
This function converts a string-based storage specification into a storage type character code used by PostgreSQL's TOAST system. It supports four storage modes: "plain" (no compression/out-of-line storage), "external" (out-of-line but no compression), "extended" (compression and out-of-line), and "main" (compression but try to keep in-line). When "default" is specified, it retrieves the default storage mode for the data type. The function includes safety checks to ensure that non-toastable data types can only use PLAIN storage mode.

## Parameters / Member Variables
- `atttypid`: The OID of the attribute's data type
- `storagemode`: String specifying the storage mode ("plain", "external", "extended", "main", or "default")

## Dependencies
- Functions called/Symbols referenced:
  - TYPSTORAGE_PLAIN
  - TYPSTORAGE_EXTERNAL
  - TYPSTORAGE_EXTENDED
  - TYPSTORAGE_MAIN
  - [get_typstorage](../g/get_typstorage.md)
  - TypeIsToastable
- Called from (representative examples):
  - [BuildDescForRelation](../B/BuildDescForRelation.md)
  - [ATExecSetStorage](../A/ATExecSetStorage.md)

## Notes and Other Information
- Uses case-insensitive string comparison for storage mode names
- Enforces that only toastable data types can use storage modes other than PLAIN
- Returns the appropriate TYPSTORAGE constant for valid storage modes
- Used during table creation and ALTER TABLE SET STORAGE operations
- Provides clear error messages for invalid storage types and unsupported combinations

## Simplified Source
```c
static char GetAttributeStorage(Oid atttypid, const char *storagemode)
{
    char cstorage = 0;

    // Convert storage mode string to storage type constant
    if (pg_strcasecmp(storagemode, "plain") == 0)
        cstorage = TYPSTORAGE_PLAIN;
    else if (pg_strcasecmp(storagemode, "external") == 0)
        cstorage = TYPSTORAGE_EXTERNAL;
    else if (pg_strcasecmp(storagemode, "extended") == 0)
        cstorage = TYPSTORAGE_EXTENDED;
    else if (pg_strcasecmp(storagemode, "main") == 0)
        cstorage = TYPSTORAGE_MAIN;
    else if (pg_strcasecmp(storagemode, "default") == 0)
        cstorage = get_typstorage(atttypid);  // Get type's default storage
    else
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid storage type \"%s\"", storagemode)));

    // Safety check: non-toastable types can only use PLAIN storage
    if (!(cstorage == TYPSTORAGE_PLAIN || TypeIsToastable(atttypid)))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("column data type %s can only have storage PLAIN",
                        format_type_be(atttypid))));

    return cstorage;
}
```