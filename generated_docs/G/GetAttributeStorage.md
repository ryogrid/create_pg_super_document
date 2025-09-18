# GetAttributeStorage

## Location
src/backend/commands/tablecmds.c: 20268 - 20299

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
  - get_typstorage
  - TypeIsToastable
- Called from (representative examples):
  - BuildDescForRelation
  - ATExecSetStorage

## Notes and Other Information
- Uses case-insensitive string comparison for storage mode names
- Enforces that only toastable data types can use storage modes other than PLAIN
- Returns the appropriate TYPSTORAGE constant for valid storage modes
- Used during table creation and ALTER TABLE SET STORAGE operations
- Provides clear error messages for invalid storage types and unsupported combinations