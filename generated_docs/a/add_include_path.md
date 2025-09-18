# add_include_path

## Location
[src/interfaces/ecpg/preproc/ecpg.c:67-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/ecpg.c#L67-L88)

## Overview
Adds a new directory path to the global linked list of include paths used by the ECPG preprocessor for finding header files.

## Definition
```c
static void add_include_path(char *path)
```

## Detailed Description
The `add_include_path` function is a utility function in the ECPG (Embedded SQL in C) preprocessor that manages include paths for header file resolution. It adds a new path to the end of a singly-linked list of include paths stored in the global `include_paths` variable. The function allocates memory for a new `_include_path` structure and properly links it to the existing list, ensuring that paths are searched in the order they were added.

## Parameters / Member Variables
- `path`: A string containing the directory path to be added to the include path list. The function stores a reference to this string rather than making a copy.

## Dependencies
- Functions called/Symbols referenced:
  - [mm_alloc](../m/mm_alloc.md) (ECPG memory allocation function)
  - struct _include_path (include path list node structure)
  - include_paths (global variable maintaining the list head)

- Called from (representative examples):
  - [main](../m/main.md) (in src/interfaces/ecpg/preproc/ecpg.c at multiple lines: 187, 219, 265, 266, 268, 269)

## Notes and Other Information
- The function is static and only accessible within the ecpg.c compilation unit
- Memory allocation is handled through mm_alloc, which is ECPG's memory management function
- The function maintains the include paths as a singly-linked list with new paths appended to the end
- No validation is performed on the input path parameter
- The function stores a reference to the provided path string rather than creating a copy, so the caller must ensure the string remains valid
- Multiple calls to this function from main() suggest it handles various standard include paths and user-specified paths