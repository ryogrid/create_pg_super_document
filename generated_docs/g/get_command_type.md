# get_command_type

## Location
[src/test/modules/test_ddl_deparse/test_ddl_deparse.c:31-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_ddl_deparse/test_ddl_deparse.c#L31-L71)

## Overview
Returns the textual representation of the command type from a CollectedCommand structure used in DDL deparsing tests.

## Definition

```c
Datum
get_command_type(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is part of the test_ddl_deparse module and serves as a utility function for testing DDL command deparsing functionality. It takes a CollectedCommand pointer and returns a human-readable string representation of the command type. The function performs a switch statement on the command type field and maps each command type enum value to its corresponding textual representation.

The function is designed to help developers and testers understand what type of DDL command has been collected during the deparsing process by providing a clear textual description rather than just the enum value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that contains a CollectedCommand pointer as the first argument
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (to extract the CollectedCommand pointer)
  - [cstring_to_text](../c/cstring_to_text.md) (to convert C string to PostgreSQL text type)
  - PG_RETURN_TEXT_P (to return the text result)
- Enum values referenced:
  - SCT_Simple
  - SCT_AlterTable
  - SCT_Grant
  - SCT_AlterOpFamily
  - SCT_AlterDefaultPrivileges
  - SCT_CreateOpClass
  - SCT_AlterTSConfig
- Called from:
  - No direct callers found (likely used as a SQL-callable function in tests)

## Notes and Other Information
- This function is specifically part of the test infrastructure for DDL deparsing
- Returns "unknown command type" for any command types not explicitly handled
- The function maps command type enums to user-friendly strings for debugging and testing purposes
- Located in the test_ddl_deparse extension module, indicating it's primarily for testing functionality

## Simplified Source

```c
Datum get_command_type(PG_FUNCTION_ARGS) {
    // Extract CollectedCommand pointer from arguments
    CollectedCommand *cmd = (CollectedCommand *) PG_GETARG_POINTER(0);
    const char *type;

    // Map command type enum to string representation
    switch (cmd->type) {
        case SCT_Simple:
            type = "simple";
            break;
        case SCT_AlterTable:
            type = "alter table";
            break;
        case SCT_Grant:
            type = "grant";
            break;
        case SCT_AlterOpFamily:
            type = "alter operator family";
            break;
        case SCT_AlterDefaultPrivileges:
            type = "alter default privileges";
            break;
        case SCT_CreateOpClass:
            type = "create operator class";
            break;
        case SCT_AlterTSConfig:
            type = "alter text search configuration";
            break;
        default:
            type = "unknown command type";
            break;
    }

    // Convert C string to PostgreSQL text and return
    PG_RETURN_TEXT_P(cstring_to_text(type));
}
```