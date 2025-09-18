# get_command_tag

## Location
[src/test/modules/test_ddl_deparse/test_ddl_deparse.c:72-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_ddl_deparse/test_ddl_deparse.c#L72-L86)

## Overview
Returns the command tag corresponding to a parse node contained in a CollectedCommand structure, used for DDL command identification in tests.

## Definition
```c
Datum get_command_tag(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of the test_ddl_deparse module and provides a way to extract the command tag from a CollectedCommand structure. It takes a CollectedCommand pointer and returns the command tag string that corresponds to the parse tree contained within the command structure. The command tag represents the type of SQL command (e.g., "CREATE TABLE", "ALTER TABLE", etc.) and is obtained by calling CreateCommandName() on the parse tree.

The function includes a null check for the parse tree and returns NULL if no parse tree is present, ensuring safe handling of incomplete command structures.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that contains a CollectedCommand pointer as the first argument

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (to extract the CollectedCommand pointer)
  - CreateCommandName (to generate the command name from the parse tree)
  - cstring_to_text (to convert C string to PostgreSQL text type)
  - PG_RETURN_TEXT_P (to return the text result)
  - PG_RETURN_NULL (to return NULL when parse tree is missing)
- Data structures referenced:
  - CollectedCommand (input structure containing parse tree)
- Called from:
  - No direct callers found (likely used as a SQL-callable function in tests)

## Notes and Other Information
- This function is specifically part of the test infrastructure for DDL deparsing
- Returns NULL if the CollectedCommand contains no parse tree, providing safe error handling
- The command tag is generated using PostgreSQL's internal CreateCommandName function
- Used in conjunction with other test functions to analyze DDL command parsing results
- Located in the test_ddl_deparse extension module for testing DDL deparsing functionality