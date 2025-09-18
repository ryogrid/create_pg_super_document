# get_pkginclude_path

## Location
src/port/path.c: 928 - 936

## Overview
Constructs the path to the PostgreSQL package-specific include directory relative to the PostgreSQL executable path.

## Definition
```c
void get_pkginclude_path(const char *my_exec_path, char *ret_path)
```

## Detailed Description
The `get_pkginclude_path` function calculates the absolute path to PostgreSQL's package-specific include directory by making a relative path calculation from the provided executable path. It uses the compile-time constants PKGINCLUDEDIR and PGBINDIR to determine the proper relative location of the package include directory. This directory typically contains PostgreSQL-specific header files that are separate from the general system include directory, often used for PostgreSQL extensions and development tools.

## Parameters / Member Variables
- `my_exec_path`: The absolute path to the current PostgreSQL executable
- `ret_path`: Output buffer where the calculated package include directory path will be stored

## Dependencies
- Functions called/Symbols referenced:
  - make_relative_path
- Called from (representative examples):
  - get_configdata (src/common/config_info.c:72)
  - main (src/interfaces/ecpg/preproc/ecpg.c:185)

## Notes and Other Information
- This function assumes that the caller has provided a sufficiently large buffer in ret_path to hold the resulting path
- The function relies on compile-time constants PKGINCLUDEDIR and PGBINDIR which are set during the build process
- PKGINCLUDEDIR typically points to a PostgreSQL-specific subdirectory within the include hierarchy (e.g., /usr/include/postgresql)
- This is part of PostgreSQL's portable path resolution system that allows development tools to locate PostgreSQL-specific header files
- Commonly used by tools like ecpg that need to distinguish between system headers and PostgreSQL-specific headers