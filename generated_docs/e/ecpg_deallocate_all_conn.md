# ecpg_deallocate_all_conn

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:337-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L337-L349)

## Overview
A utility function that deallocates all prepared statements associated with a specific database connection in the ECPG library.

## Definition

```c
bool
ecpg_deallocate_all_conn(int lineno, enum COMPAT_MODE c, struct connection *con)
```
## Detailed Description
 provides a bulk deallocation mechanism for cleaning up all prepared statements associated with a particular database connection. It iterates through the connection's linked list of prepared statements, calling  for each statement until the list is empty. This function is typically used during connection cleanup or when an application needs to reset all prepared statements for a connection. The function ensures complete cleanup by continuing until all statements are removed from the connection's prep_stmts list.

## Parameters / Member Variables
- `lineno`: Source code line number where the bulk deallocation was initiated (for error reporting and debugging)
- `c`: Compatibility mode enumeration that affects error handling behavior during individual statement deallocations
- `*con`: Pointer to the database connection structure whose prepared statements should be deallocated
## Dependencies
- Functions called/Symbols referenced:
  - [deallocate_one](../d/deallocate_one.md) (perform deallocation of individual statements)
  - COMPAT_MODE (compatibility mode enumeration)
- Called from (representative examples):
  - [ecpg_finish](ecpg_finish.md) (during connection cleanup)
  - [ECPGdeallocate_all](../E/ECPGdeallocate_all.md) (public API for deallocating all statements)

## Notes and Other Information
- Returns true if all statements were successfully deallocated, false if any deallocation failed
- The function stops and returns false on the first failed deallocation, leaving remaining statements allocated
- Used internally for connection cleanup during disconnection or application termination
- The loop continues until con->prep_stmts becomes NULL, indicating all statements have been removed
- Each successful call to deallocate_one automatically updates con->prep_stmts to point to the next statement in the list
- Essential for preventing memory leaks when closing connections or resetting connection state

## Simplified Source

```c
bool ecpg_deallocate_all_conn(int lineno, enum COMPAT_MODE c, struct connection *con) {
    // Deallocate all prepared statements in the connection
    while (con->prep_stmts) {
        // Deallocate one statement at a time
        if (!deallocate_one(lineno, c, con, NULL, con->prep_stmts))
            return false;
    }

    return true;
}
```