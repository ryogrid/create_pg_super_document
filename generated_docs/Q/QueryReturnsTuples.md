# QueryReturnsTuples

## Location
src/backend/tcop/utility.c: 2135 - 2176

## Overview
QueryReturnsTuples determines whether a parsed query will produce tuple output, serving as the primary interface for checking if any type of SQL command returns tabular results.

## Definition

```c
bool
QueryReturnsTuples(Query *parsetree)
```
## Detailed Description
QueryReturnsTuples is a comprehensive function that analyzes a parsed query to determine if it will produce tuple output. It serves as a higher-level wrapper that handles both regular SQL commands and utility statements. The function examines the command type and applies specific logic:

- **SELECT**: Always returns true as SELECT statements inherently return tuples
- **INSERT/UPDATE/DELETE/MERGE**: Returns true only if they have a RETURNING clause, which causes these DML statements to output the affected rows
- **UTILITY**: Delegates to UtilityReturnsTuples() to handle utility commands like EXPLAIN, SHOW, CALL, etc.
- **UNKNOWN/NOTHING**: Returns false for these edge cases

This function is essential for the query processing infrastructure to determine the appropriate execution strategy and result handling mechanisms.

## Parameters / Member Variables
- : Pointer to a Query structure representing the parsed SQL command

## Dependencies
- Functions called/Symbols referenced:
  - UtilityReturnsTuples (for utility command analysis)
  - Command type constants: CMD_SELECT, CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE, CMD_UTILITY, CMD_UNKNOWN, CMD_NOTHING

- Called from:
  - Currently no direct callers found in the analyzed codebase, but likely used by query planning and execution infrastructure

## Notes and Other Information
- This function provides a unified interface for determining tuple output across all command types
- The RETURNING clause detection for DML statements enables these commands to behave like SELECT in terms of output
- For utility commands, the function delegates to the specialized UtilityReturnsTuples() function
- Edge cases like CMD_UNKNOWN and CMD_NOTHING are handled gracefully by returning false
- Part of the query processing pipeline that helps optimize execution strategies based on expected output