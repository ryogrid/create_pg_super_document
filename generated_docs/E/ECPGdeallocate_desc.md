# ECPGdeallocate_desc

## Location
src/interfaces/ecpg/ecpglib/descriptor.c: 748 - 779

## Overview
Deallocates a SQL descriptor by name, removing it from the global descriptor list and freeing its associated memory.

## Definition
```c
bool ECPGdeallocate_desc(int line, const char *name)
```

## Detailed Description
This function searches for a descriptor with the specified name in the global descriptor list and deallocates it. The function performs a linear search through the linked list of descriptors, and when found, removes the descriptor from the list and frees its memory using `descriptor_free()`. If the descriptor is not found, an error is raised. The function ensures proper error handling by initializing the SQLCA structure and checking for out-of-memory conditions.

## Parameters / Member Variables
- `line`: Line number for error reporting purposes in the ECPG preprocessor context
- `name`: Name of the descriptor to deallocate (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca: Gets the SQLCA structure for error handling
  - ecpg_init_sqlca: Initializes the SQLCA structure
  - get_descriptors: Retrieves the head of the descriptor list
  - set_descriptors: Updates the head of the descriptor list
  - descriptor_free: Frees memory associated with a descriptor
  - ecpg_raise: Raises ECPG errors with appropriate error codes
  - ECPG_OUT_OF_MEMORY: Error constant for memory allocation failures
  - ECPG_UNKNOWN_DESCRIPTOR: Error constant for unknown descriptor names
  - ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY: SQL state for out-of-memory conditions
  - ECPG_SQLSTATE_INVALID_SQL_DESCRIPTOR_NAME: SQL state for invalid descriptor names

- Called from (representative examples):
  - Various test programs in src/interfaces/ecpg/test/expected/
  - ECPG-generated code for DEALLOCATE DESCRIPTOR statements

## Notes and Other Information
- Returns `true` on successful deallocation, `false` on error
- The function maintains the integrity of the global descriptor linked list during removal
- Error conditions include: SQLCA allocation failure, descriptor not found
- This function is part of the ECPG (Embedded SQL in C) library for PostgreSQL
- Thread-safe as evidenced by usage in thread-descriptor tests
- The function is typically called by ECPG-generated code when processing DEALLOCATE DESCRIPTOR SQL statements