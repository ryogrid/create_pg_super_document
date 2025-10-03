# get_html_path

## Location
[src/port/path.c:982-990](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L982-L990)

## Overview
Constructs the full path to PostgreSQL's HTML documentation directory by calculating a relative path from the executable location.

## Definition

```c
void
get_html_path(const char *my_exec_path, char *ret_path)
```
## Detailed Description
This function determines the absolute path to PostgreSQL's HTML documentation directory based on the location of the current executable. It uses the  function to compute the path by using the compile-time constants HTMLDIR and PGBINDIR to establish the relative relationship between the binary directory and the HTML documentation directory.

The function enables PostgreSQL utilities and applications to locate HTML documentation files at runtime, supporting web-based help systems and documentation browsers in relocatable installations where the actual installation path may differ from the compile-time configured paths.

## Parameters / Member Variables
- `*my_exec_path`: Input parameter containing the full path to the current executable
- `*ret_path`: Output buffer where the computed HTML documentation directory path will be stored (must be at least MAXPGPATH bytes)
## Dependencies
- Functions called/Symbols referenced:
  - [make_relative_path](../m/make_relative_path.md)
  - HTMLDIR (compile-time constant)
  - PGBINDIR (compile-time constant)
- Called from (representative examples):
  - [get_configdata](get_configdata.md) (src/common/config_info.c:60)

## Notes and Other Information
- Part of PostgreSQL's installation path resolution system for documentation
- Specifically targets HTML-formatted documentation files
- The function assumes ret_path buffer is sufficiently large (MAXPGPATH)
- Used by configuration reporting tools to locate HTML documentation
- Supports web-based documentation viewing in PostgreSQL administration tools
- Essential for relocatable installations where documentation paths need dynamic resolution