# get_include_path

## Location
src/port/path.c: 919 - 927

## Overview
Constructs the path to the PostgreSQL include directory relative to the PostgreSQL executable path.

## Definition
```c
void get_include_path(const char *my_exec_path, char *ret_path)
```

## Detailed Description
The `get_include_path` function calculates the absolute path to PostgreSQL's include directory by making a relative path calculation from the provided executable path. It uses the compile-time constants INCLUDEDIR and PGBINDIR to determine the proper relative location of the include directory. This function is primarily used by PostgreSQL development tools and utilities that need to locate header files for compilation or configuration purposes.

## Parameters / Member Variables
- `my_exec_path`: The absolute path to the current PostgreSQL executable
- `ret_path`: Output buffer where the calculated include directory path will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [make_relative_path](../m/make_relative_path.md)
- Called from (representative examples):
  - [get_configdata](get_configdata.md) (src/common/config_info.c:66)
  - [main](../m/main.md) (src/interfaces/ecpg/preproc/ecpg.c:267)

## Notes and Other Information
- This function assumes that the caller has provided a sufficiently large buffer in ret_path to hold the resulting path
- The function relies on compile-time constants INCLUDEDIR and PGBINDIR which are set during the build process
- This is part of PostgreSQL's portable path resolution system that allows development tools to locate header files correctly
- Commonly used by tools like ecpg (Embedded SQL preprocessor) that need to find PostgreSQL header files