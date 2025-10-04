# get_includeserver_path

## Location
[src/port/path.c:937-945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L937-L945)

## Overview
Constructs the path to the PostgreSQL server-specific include directory relative to the PostgreSQL executable path.

## Definition
```c
void get_includeserver_path(const char *my_exec_path, char *ret_path)
```

## Detailed Description
The `get_includeserver_path` function calculates the absolute path to PostgreSQL's server-specific include directory by making a relative path calculation from the provided executable path. It uses the compile-time constants INCLUDEDIRSERVER and PGBINDIR to determine the proper relative location of the server include directory. This directory contains header files specifically needed for server-side development, such as extension development and server internal programming interfaces.

## Parameters / Member Variables
- `my_exec_path`: The absolute path to the current PostgreSQL executable
- `ret_path`: Output buffer where the calculated server include directory path will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [make_relative_path](../m/make_relative_path.md)
- Called from (representative examples):
  - [get_configdata](get_configdata.md) (src/common/config_info.c:78)

## Notes and Other Information
- This function assumes that the caller has provided a sufficiently large buffer in ret_path to hold the resulting path
- The function relies on compile-time constants INCLUDEDIRSERVER and PGBINDIR which are set during the build process
- INCLUDEDIRSERVER typically points to a server-specific subdirectory within the include hierarchy (e.g., /usr/include/postgresql/server)
- This directory is crucial for PostgreSQL extension development as it contains internal server headers
- This is part of PostgreSQL's portable path resolution system that allows development tools to locate server-specific header files
- Distinguished from client-side includes, this path is specifically for server-side development needs

## Simplified Source

```c
void
get_includeserver_path(const char *my_exec_path, char *ret_path)
{
    // Calculate relative path from executable to server include directory
    make_relative_path(ret_path, INCLUDEDIRSERVER, PGBINDIR, my_exec_path);
}
```