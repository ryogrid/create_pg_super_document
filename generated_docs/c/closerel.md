# closerel

## Location
[src/backend/bootstrap/bootstrap.c:453-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L453-L489)

## Overview
closerel closes the currently opened relation in PostgreSQL's bootstrap mode, providing validation and cleanup of the global relation descriptor state.

## Definition
```c
void closerel(char *relname)
```

## Detailed Description
closerel is responsible for properly closing relations that were previously opened during PostgreSQL's bootstrap process. The function provides robust validation and error checking to ensure proper bootstrap state management.

Key functionality includes:
1. **Name validation**: When a relation name is provided, it validates that the currently opened relation matches the expected name, preventing mismatched close operations
2. **State validation**: Ensures that a relation is actually open before attempting to close it, preventing invalid operations
3. **Resource cleanup**: Uses the table access method (table_close) to properly release the relation with no locking required
4. **State reset**: Clears the global boot_reldesc pointer to indicate no relation is currently open

The function supports both explicit closes (with relname parameter) and implicit closes (with NULL relname), making it flexible for different bootstrap scenarios.

## Parameters / Member Variables
- `relname`: Name of the relation to close (can be NULL for implicit close operations)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelationName
  - elog (for error reporting and debug output)  
  - [table_close](../t/table_close.md)
  - NoLock (locking constant)
- Called from (representative examples):
  - [boot_openrel](../b/boot_openrel.md) (to close previously opened relations)
  - [DefineAttr](../D/DefineAttr.md) (during attribute definition)
  - [cleanup](cleanup.md) (during bootstrap cleanup)

## Notes and Other Information
- Uses global variable boot_reldesc to track the currently opened relation
- Provides comprehensive error checking with descriptive error messages
- When relname is provided, validates it matches the currently opened relation name
- When relname is NULL, simply closes whatever relation is currently open
- Includes DEBUG4 logging to track relation closure operations
- Uses NoLock when closing relations since bootstrap runs in single-user mode
- Located in src/backend/bootstrap/bootstrap.c:453-489
- Part of the bootstrap relation management system alongside boot_openrel
- Essential for maintaining proper resource management during bootstrap operations