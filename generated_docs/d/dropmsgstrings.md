# dropmsgstrings

## Location
src/backend/commands/tablecmds.c: 242 - 310

## Overview
dropmsgstrings is a structure that provides standardized error messages for different types of database objects during DROP operations in PostgreSQL. It contains templates for various error conditions that can occur when attempting to drop relations.

## Definition
```c
struct dropmsgstrings
{
    char        kind;
    int         nonexistent_code;
    const char *nonexistent_msg;
    const char *skipping_msg;
    const char *nota_msg;
    const char *drophint_msg;
};
```

## Detailed Description
The dropmsgstrings structure is used to provide context-appropriate error messages when DROP operations fail. It is part of PostgreSQL's error-reporting infrastructure for the RemoveRelations functionality. Each instance of this structure corresponds to a specific relation kind (table, sequence, view, etc.) and contains the appropriate error messages and error codes for various failure scenarios. The structure is typically used as part of a static array (dropmsgstringarray) that maps relation kinds to their corresponding error message templates.

## Parameters / Member Variables
- `kind`: The relation kind character (RELKIND_RELATION, RELKIND_SEQUENCE, etc.) this message set applies to
- `nonexistent_code`: The PostgreSQL error code to use when the object doesn't exist
- `nonexistent_msg`: Error message template for when the specified object does not exist
- `skipping_msg`: Error message template for when the object doesn't exist but IF EXISTS was specified
- `nota_msg`: Error message template for when the object exists but is not of the expected type
- `drophint_msg`: Hint message suggesting the correct DROP command to use for this object type

## Dependencies
- Functions called/Symbols referenced:
  - Various RELKIND_* constants (RELKIND_RELATION, RELKIND_SEQUENCE, etc.)
  - Various ERRCODE_* constants (ERRCODE_UNDEFINED_TABLE, etc.)
- Called from (representative examples):
  - [DropErrorMsgNonExistent](../D/DropErrorMsgNonExistent.md)
  - [DropErrorMsgWrongType](../D/DropErrorMsgWrongType.md)

## Notes and Other Information
- Used specifically for error reporting in DROP operations
- Part of PostgreSQL's internationalization (i18n) system with gettext_noop macros
- Provides consistent error messages across different object types
- The structure supports PostgreSQL's IF EXISTS syntax with appropriate "skipping" messages
- Used in conjunction with dropmsgstringarray, a static array containing predefined message sets for all supported relation kinds