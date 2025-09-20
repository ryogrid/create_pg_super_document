# initialize_ntdll

## Location
[src/port/win32ntdll.c:39-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32ntdll.c#L39-L71)

## Overview
Dynamically loads Windows NT functions from ntdll.dll and initializes function pointers for use by PostgreSQL on Windows systems.

## Definition

```c
int
initialize_ntdll(void)
```
## Detailed Description
The  function is responsible for dynamically loading the Windows NT library (ntdll.dll) and resolving function addresses for specific NT functions that PostgreSQL needs to access. This approach allows PostgreSQL to use advanced Windows NT functions while maintaining compatibility across different Windows versions.

The function uses a lazy initialization pattern - it only loads the library once and sets a static  flag to prevent repeated loading. It iterates through a predefined array of routine names, resolves each function's address using , and stores the addresses in corresponding function pointers.

If any step fails (library loading or function resolution), the function performs cleanup by freeing the loaded library and returns an error code. This ensures that PostgreSQL can gracefully handle systems where these NT functions might not be available.

## Parameters / Member Variables
This function takes no parameters and returns:
- : Success - all NT functions were successfully loaded and initialized
- : Failure - library loading failed or one or more functions could not be resolved

## Dependencies
- Functions called/Symbols referenced:
  -  (Windows API)
  -  (Windows API)
  -  (Windows API)
  -  (Windows API)
  -  (PostgreSQL utility function)
  -  (PostgreSQL macro)
  -  (static array of NtDllRoutine structures)

- Called from (representative examples):
  -  (src/port/open.c:71)
  -  (src/port/win32fdatasync.c:36)
  - Referenced in  context (src/include/port/win32ntdll.h:32)

## Notes and Other Information
- The function is designed for Windows-specific functionality and is part of PostgreSQL's portability layer
- Uses static initialization to ensure the library is only loaded once per process
- The specific NT functions being loaded are: , , and 
- Error handling converts Windows error codes to DOS error codes using 
- This functionality is critical for advanced file I/O operations on Windows, particularly for implementing reliable data synchronization