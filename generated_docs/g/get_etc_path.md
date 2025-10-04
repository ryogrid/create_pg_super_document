# get_etc_path

## Location
[src/port/path.c:910-918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L910-L918)

## Overview
Constructs the path to the PostgreSQL configuration directory (etc) relative to the PostgreSQL executable path.

## Definition

```c
void
get_etc_path(const char *my_exec_path, char *ret_path)
```
## Detailed Description
The  function calculates the absolute path to PostgreSQL's configuration directory by making a relative path calculation from the provided executable path. It uses the compile-time constants SYSCONFDIR and PGBINDIR to determine the proper relative location of the configuration directory. This function is essential for PostgreSQL components to locate configuration files like postgresql.conf when the installation location may vary from the compiled-in defaults.

## Parameters / Member Variables
- `*my_exec_path`: The absolute path to the current PostgreSQL executable
- `*ret_path`: Output buffer where the calculated configuration directory path will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [make_relative_path](../m/make_relative_path.md)
- Called from (representative examples):
  - [process_psqlrc](../p/process_psqlrc.md) (src/bin/psql/startup.c:785)
  - [get_configdata](get_configdata.md) (src/common/config_info.c:114)
  - [set_pglocale_pgservice](../s/set_pglocale_pgservice.md) (src/common/exec.c:482)

## Notes and Other Information
- This function assumes that the caller has provided a sufficiently large buffer in ret_path to hold the resulting path
- The function relies on compile-time constants SYSCONFDIR and PGBINDIR which are set during the build process
- This is part of PostgreSQL's portable path resolution system that allows the software to work correctly even when moved from its original installation location

## Simplified Source

```c
void
get_etc_path(const char *my_exec_path, char *ret_path)
{
    // Calculate relative path from executable to config directory
    make_relative_path(ret_path, SYSCONFDIR, PGBINDIR, my_exec_path);
}
```