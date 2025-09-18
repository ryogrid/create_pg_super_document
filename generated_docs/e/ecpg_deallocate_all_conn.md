# ecpg_deallocate_all_conn

## Location
src/interfaces/ecpg/ecpglib/prepare.c: 337 - 349

## Overview
A utility function that deallocates all prepared statements associated with a specific database connection in the ECPG library.

## Definition


## Detailed Description
 provides a bulk deallocation mechanism for cleaning up all prepared statements associated with a particular database connection. It iterates through the connection's linked list of prepared statements, calling  for each statement until the list is empty. This function is typically used during connection cleanup or when an application needs to reset all prepared statements for a connection. The function ensures complete cleanup by continuing until all statements are removed from the connection's prep_stmts list.

## Parameters / Member Variables
- : Source code line number where the bulk deallocation was initiated (for error reporting and debugging)
- : Compatibility mode enumeration that affects error handling behavior during individual statement deallocations
- : Pointer to the database connection structure whose prepared statements should be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - deallocate_one (perform deallocation of individual statements)
  - COMPAT_MODE (compatibility mode enumeration)
- Called from (representative examples):
  - ecpg_finish (during connection cleanup)
  - ECPGdeallocate_all (public API for deallocating all statements)

## Notes and Other Information
- Returns true if all statements were successfully deallocated, false if any deallocation failed
- The function stops and returns false on the first failed deallocation, leaving remaining statements allocated
- Used internally for connection cleanup during disconnection or application termination
- The loop continues until con->prep_stmts becomes NULL, indicating all statements have been removed
- Each successful call to deallocate_one automatically updates con->prep_stmts to point to the next statement in the list
- Essential for preventing memory leaks when closing connections or resetting connection state