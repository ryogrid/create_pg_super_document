# get_pkglib_path

## Location
[src/port/path.c:955-963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L955-L963)

## Overview
Constructs the full path to PostgreSQL's package library directory by calculating a relative path from the executable location.

## Definition

```c
void
get_pkglib_path(const char *my_exec_path, char *ret_path)
```
## Detailed Description
This function determines the absolute path to PostgreSQL's package library directory (pkglibdir) based on the location of the current executable. It uses the  function to compute the path by using the compile-time constants PKGLIBDIR and PGBINDIR to establish the relative relationship between the binary directory and the package library directory.

The function is essential for PostgreSQL installations to locate shared libraries and extension modules at runtime, particularly in relocatable installations where the actual installation path may differ from the compile-time paths.

## Parameters / Member Variables
- `*my_exec_path`: Input parameter containing the full path to the current executable
- `*ret_path`: Output buffer where the computed package library path will be stored (must be at least MAXPGPATH bytes)
## Dependencies
- Functions called/Symbols referenced:
  - [make_relative_path](../m/make_relative_path.md)
  - PKGLIBDIR (compile-time constant)
  - PGBINDIR (compile-time constant)
- Called from (representative examples):
  - [getInstallationPaths](getInstallationPaths.md) (src/backend/postmaster/postmaster.c:1457)
  - [InitStandaloneProcess](../I/InitStandaloneProcess.md) (src/backend/utils/init/miscinit.c:218)
  - [get_configdata](get_configdata.md) (src/common/config_info.c:90, 120)

## Notes and Other Information
- Part of PostgreSQL's path resolution system for relocatable installations
- The function assumes ret_path buffer is sufficiently large (MAXPGPATH)
- Used during server startup and by utilities that need to locate shared libraries
- The actual path computation is delegated to make_relative_path which handles the complex logic of path resolution

## Simplified Source

```c
// Simplified version of get_pkglib_path
void get_pkglib_path(const char *my_exec_path, char *ret_path) {
    // Calculate path from executable location to package library directory
    // Uses compile-time constants PKGLIBDIR and PGBINDIR to determine relative path
    make_relative_path(ret_path, PKGLIBDIR, PGBINDIR, my_exec_path);
}
```

Key simplifications made:
- Added explanatory comments for the single function call
- Clarified the purpose of the compile-time constants
- Function is already at optimal simplicity as a thin wrapper