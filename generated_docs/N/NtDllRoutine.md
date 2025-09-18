# NtDllRoutine

## Location
[src/port/win32ntdll.c:24-28](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32ntdll.c#L24-L28)

## Overview
A structure used to define the mapping between Windows NT function names and their corresponding function pointer addresses for dynamic loading.

## Definition


## Detailed Description
The  structure serves as a lookup table entry for dynamically loading Windows NT functions from ntdll.dll. It provides a clean abstraction for managing the relationship between function names (as strings) and their runtime addresses (as function pointers).

This structure is used in conjunction with the  function to create a table-driven approach for loading multiple NT functions. Each entry in the routines array contains the name of an NT function and a pointer to where its resolved address should be stored.

The design allows PostgreSQL to maintain a list of required NT functions in a declarative manner, making it easy to add new functions or modify the loading process without changing the core loading logic.

## Parameters / Member Variables
- : A constant string containing the exact name of the NT function as it appears in ntdll.dll (e.g., "RtlGetLastNtStatus")
- : A pointer to a function pointer where the resolved address of the NT function will be stored after successful loading

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure definition)

- Called from (representative examples):
  - Used in the static  array in src/port/win32ntdll.c
  - Referenced by  function during dynamic loading process

## Notes and Other Information
- The structure is defined as a typedef, making it easy to declare arrays and variables of this type
- Currently used for three NT functions: , , and 
- The  type is PostgreSQL's standard function pointer type, ensuring type safety across the codebase
- This approach provides a flexible foundation for extending PostgreSQL's use of Windows NT functions as needed
- The structure supports PostgreSQL's cross-platform design by encapsulating Windows-specific functionality in a clean interface