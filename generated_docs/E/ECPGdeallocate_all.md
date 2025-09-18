# ECPGdeallocate_all

## Location
src/interfaces/ecpg/ecpglib/prepare.c: 350 - 356

## Overview
A public API function that deallocates all prepared statements for a specified database connection in the ECPG library.

## Definition


## Detailed Description
 serves as the primary public interface for performing bulk deallocation of all prepared statements associated with a named database connection. This function acts as a simple wrapper around , providing a convenient API for applications that need to clean up all prepared statements for a specific connection. It resolves the connection by name and delegates the actual deallocation work to the internal function, making it suitable for use in application code that needs to perform comprehensive cleanup operations.

## Parameters / Member Variables
- : Source code line number where the bulk deallocation was requested (for error reporting and debugging)
- : Integer representation of the compatibility mode affecting error handling behavior
- : Name of the database connection whose prepared statements should be deallocated (can be NULL for default connection)

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_deallocate_all_conn](../e/ecpg_deallocate_all_conn.md) (internal function that performs the bulk deallocation)
  - ecpg_get_connection (retrieve connection structure by name)
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