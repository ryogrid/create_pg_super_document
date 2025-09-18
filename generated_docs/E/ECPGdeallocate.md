# ECPGdeallocate

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:315-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L315-L336)

## Overview
The main public interface function for deallocating a named prepared statement in the ECPG library, handling the SQL DEALLOCATE PREPARE statement.

## Definition


## Detailed Description
 serves as the primary entry point for deallocating prepared statements in ECPG applications. It implements the functionality behind the SQL DEALLOCATE PREPARE statement by locating the specified prepared statement by name within the given connection context and delegating the actual deallocation work to the  function. The function includes proper error handling for cases where the prepared statement cannot be found, with different behavior depending on the compatibility mode.

## Parameters / Member Variables
- : Source code line number where the DEALLOCATE command originated (for error reporting and debugging)
- : Integer representation of the compatibility mode that affects error handling behavior
- : Name of the database connection where the prepared statement exists (can be NULL for default connection)
- : Name of the prepared statement to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_get_connection (retrieve connection by name)
  - [ecpg_init](../e/ecpg_init.md) (initialize connection state)
  - [ecpg_find_prepared_statement](../e/ecpg_find_prepared_statement.md) (locate statement in connection's statement list)
  - [deallocate_one](../d/deallocate_one.md) (perform actual deallocation)
  - INFORMIX_MODE (compatibility mode check)
  - [ecpg_raise](../e/ecpg_raise.md) (error reporting)
- Called from (representative examples):
  - Various test programs and ECPG-generated code
  - Main application functions using prepared statements
  - Thread-safe prepared statement management functions

## Notes and Other Information
- Returns true on successful deallocation, false on error
- In INFORMIX compatibility mode, missing statements are silently ignored (returns true)
- In standard mode, attempting to deallocate a non-existent statement raises an ECPG_INVALID_STMT error
- The function is thread-safe when used with properly isolated connection contexts
- Part of the public ECPG API and widely used in embedded SQL applications