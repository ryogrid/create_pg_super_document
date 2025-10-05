# _bt_end_vacuum_callback

## Location
[src/backend/access/nbtree/nbtutils.c:4513-4521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4513-L4521)

## Overview
A callback wrapper function for  designed to be used with PostgreSQL's error cleanup mechanism.

## Definition

```c
void
_bt_end_vacuum_callback(int code, Datum arg)
```
## Detailed Description
This function serves as an adapter that wraps  in the standard PostgreSQL callback interface. It is specifically designed to be used with PostgreSQL's error cleanup system, particularly with  macro, to ensure that VACUUM tracking resources are properly cleaned up even when errors or FATAL conditions occur.

The function converts the generic  argument back to a  pointer and calls  to perform the actual cleanup. This design ensures that B-tree VACUUM tracking slots are never permanently leaked, even in exceptional circumstances.

## Parameters / Member Variables
- `code`: Exit/error code (unused but required by callback interface)
- `arg`: Datum containing the Relation pointer, typically set using
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_end_vacuum](_bt_end_vacuum.md)
  - [DatumGetPointer](../D/DatumGetPointer.md) (macro for converting Datum to pointer)
- Called from (representative examples):
  - [btbulkdelete](btbulkdelete.md) (via error cleanup mechanism)
  - Error cleanup infrastructure (PG_ENSURE_ERROR_CLEANUP)

## Notes and Other Information
- Essential for preventing resource leaks in error scenarios
- Used with PG_ENSURE_ERROR_CLEANUP to guarantee cleanup execution
- Follows PostgreSQL's standard callback function signature (int, Datum)
- The  parameter is ignored as cleanup is always performed regardless of exit reason
- Critical for maintaining system stability by preventing permanent slot exhaustion in btvacinfo array

## Simplified Source

```c
void
_bt_end_vacuum_callback(int code, Datum arg)
{
    // Convert Datum back to relation pointer and cleanup vacuum tracking
    _bt_end_vacuum((Relation) DatumGetPointer(arg));
}
```