# ECPGdeallocate_all

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:350-356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L350-L356)

## Overview
A public API function that deallocates all prepared statements for a specified database connection in the ECPG library.

## Definition

```c
bool
ECPGdeallocate_all(int lineno, int compat, const char *connection_name)
```
## Detailed Description
 serves as the primary public interface for performing bulk deallocation of all prepared statements associated with a named database connection. This function acts as a simple wrapper around , providing a convenient API for applications that need to clean up all prepared statements for a specific connection. It resolves the connection by name and delegates the actual deallocation work to the internal function, making it suitable for use in application code that needs to perform comprehensive cleanup operations.

## Parameters / Member Variables
- `lineno`: Source code line number where the bulk deallocation was requested (for error reporting and debugging)
- `compat`: Integer representation of the compatibility mode affecting error handling behavior
- `*connection_name`: Name of the database connection whose prepared statements should be deallocated (can be NULL for default connection)
## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_deallocate_all_conn](../e/ecpg_deallocate_all_conn.md) (internal function that performs the bulk deallocation)
  - [ecpg_get_connection](../e/ecpg_get_connection.md) (retrieve connection structure by name)
- Called from (representative examples):
  - Test programs for ECPG functionality
  - Application cleanup routines
  - Connection management code

## Notes and Other Information
- Returns true if all statements were successfully deallocated, false if any deallocation failed
- Part of the public ECPG API, declared in ecpglib.h
- Provides a clean interface for applications without requiring direct access to connection structures
- The function will fail if the specified connection_name does not exist
- Commonly used during application shutdown or when resetting connection state
- More convenient than manually iterating through and deallocating individual prepared statements

## Simplified Source

```c
bool ECPGdeallocate_all(int lineno, int compat, const char *connection_name) {
    // Simple wrapper - get connection and delegate to internal function
    return ecpg_deallocate_all_conn(lineno, compat,
                                   ecpg_get_connection(connection_name));
}
```