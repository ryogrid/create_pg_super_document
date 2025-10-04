# get_doc_path

## Location
[src/port/path.c:973-981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L973-L981)

## Overview
Constructs the full path to PostgreSQL's documentation directory by calculating a relative path from the executable location.

## Definition

```c
void
get_doc_path(const char *my_exec_path, char *ret_path)
```
## Detailed Description
This function determines the absolute path to PostgreSQL's documentation directory based on the location of the current executable. It uses the  function to compute the path by using the compile-time constants DOCDIR and PGBINDIR to establish the relative relationship between the binary directory and the documentation directory.

The function enables PostgreSQL utilities and applications to locate documentation files at runtime, supporting relocatable installations where the actual installation path may differ from the compile-time configured paths.

## Parameters / Member Variables
- `*my_exec_path`: Input parameter containing the full path to the current executable
- `*ret_path`: Output buffer where the computed documentation directory path will be stored (must be at least MAXPGPATH bytes)
## Dependencies
- Functions called/Symbols referenced:
  - [make_relative_path](../m/make_relative_path.md)
  - DOCDIR (compile-time constant)
  - PGBINDIR (compile-time constant)
- Called from (representative examples):
  - [get_configdata](get_configdata.md) (src/common/config_info.c:54)

## Notes and Other Information
- Part of PostgreSQL's installation path resolution system
- Primarily used by configuration reporting and help systems
- The function assumes ret_path buffer is sufficiently large (MAXPGPATH)
- Enables dynamic location of documentation files in relocatable installations

## Simplified Source

```c
void get_doc_path(const char *my_exec_path, char *ret_path) {
    // Calculate documentation directory path relative to executable location
    make_relative_path(ret_path, DOCDIR, PGBINDIR, my_exec_path);
}
```