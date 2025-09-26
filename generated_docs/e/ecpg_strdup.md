# ecpg_strdup

## Location
[src/interfaces/ecpg/ecpglib/memory.c:47-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/memory.c#L47-L64)

## Overview
Creates a duplicate copy of a string with null pointer safety, error handling, and line number tracking for debugging in the ECPG library.

## Definition

```c
struct auto_mem
{
	void	   *pointer;
	struct auto_mem *next;
};
```
## Detailed Description
The  function provides a safe wrapper around the standard  function, offering string duplication with comprehensive error handling and debugging support. This function is essential for creating independent copies of strings in ECPG operations, particularly when managing connection parameters, SQL statements, and other string data.

The function includes explicit null pointer checking - if the input string is NULL, it returns NULL without attempting duplication, avoiding potential crashes. When string duplication fails due to memory allocation issues, the function raises an ECPG error with the specific line number where the duplication was attempted.

Unlike the standard , this function ensures proper error reporting through ECPG's error handling system, making it easier to debug memory-related issues in embedded SQL applications.

## Parameters / Member Variables
- `*pointer`: Pointer to the null-terminated string to duplicate (can be NULL)
- `*next`: Line number in the source code where the string duplication is requested, used for error reporting and debugging
## Dependencies
- Functions called/Symbols referenced:
  - strdup (standard C library function)
  - [ecpg_raise](ecpg_raise.md) (ECPG error reporting function)
  - ECPG_OUT_OF_MEMORY (error constant)
  - ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY (SQL state constant)
- Called from (representative examples):
  - [ECPGconnect](../E/ECPGconnect.md) (connection string handling)
  - [ECPGget_desc](../E/ECPGget_desc.md) (descriptor name handling)
  - [ecpg_store_input](ecpg_store_input.md) (string parameter processing)
  - [ecpg_do_prologue](ecpg_do_prologue.md) (statement and connection name handling)
  - [ecpg_register_prepared_stmt](ecpg_register_prepared_stmt.md) (prepared statement name handling)
  - [prepare_common](../p/prepare_common.md) (SQL statement duplication)
  - [AddStmtToCache](../A/AddStmtToCache.md) (statement caching)

## Notes and Other Information
- Returns NULL if the input string is NULL, providing safe null pointer handling
- Returns NULL on duplication failure after raising an appropriate error
- The duplicated string must be freed using ecpg_free() to maintain consistency in memory management
- The line number parameter enables precise error location tracking in complex ECPG applications
- Extensively used throughout ECPG for connection management, descriptor handling, statement processing, and parameter management
- Particularly important for managing connection parameters, SQL statement text, and prepared statement names
- Part of ECPG's comprehensive memory management system that ensures safe string handling in embedded SQL applications