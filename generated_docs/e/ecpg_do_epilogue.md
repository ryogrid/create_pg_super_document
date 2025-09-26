# ecpg_do_epilogue

## Location
[src/interfaces/ecpg/ecpglib/execute.c:2211-2242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L2211-L2242)

## Overview
Restores the application locale settings and frees statement structures during cleanup phase of ECPG statement execution.

## Definition

```c
void
ecpg_do_epilogue(struct statement *stmt)
```
## Detailed Description
This function performs essential cleanup tasks after ECPG statement execution or when errors occur during statement initialization. It serves as the counterpart to ecpg_do_prologue and ensures proper resource management and locale restoration.

**Key responsibilities:**
- Restores the original numeric locale that was changed for database communication
- Handles both thread-safe (uselocale) and traditional (setlocale) locale management
- Manages Windows-specific thread locale configuration via _configthreadlocale
- Safely deallocates the entire statement structure and associated resources
- Provides safe null-pointer handling for robust error recovery

The function uses conditional compilation to handle different locale management approaches across platforms, ensuring proper cleanup regardless of the available system functions.

## Parameters / Member Variables
- : Pointer to statement structure to be cleaned up (safely handles NULL pointers)

## Dependencies
- Functions called/Symbols referenced:
  - uselocale: Restores thread-specific locale (when HAVE_USELOCALE defined)
  - setlocale: Restores global numeric locale (fallback method)
  - _configthreadlocale: Restores Windows thread locale configuration
  - free_statement: Deallocates statement structure and all associated memory
  - locale_t: Locale type for thread-safe locale operations
- Called from (representative examples):
  - ecpg_do_prologue: Multiple times during error handling
  - ecpg_do: Final cleanup after statement execution

## Notes and Other Information
- Returns void - always succeeds and performs cleanup operations
- Thread-safe cleanup using uselocale() when available for better concurrency
- Safely handles partially initialized statements during error recovery
- Designed to be called multiple times safely (idempotent for NULL statements)
- Critical for preventing memory leaks and locale corruption
- Windows-specific handling ensures proper thread locale restoration
- Essential component of ECPG's resource management system
- Always paired with ecpg_do_prologue in the statement lifecycle