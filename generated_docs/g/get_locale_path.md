# get_locale_path

## Location
[src/port/path.c:964-972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L964-L972)

## Overview
Constructs the full path to PostgreSQL's locale directory by calculating a relative path from the executable location.

## Definition

```c
void
get_locale_path(const char *my_exec_path, char *ret_path)
```
## Detailed Description
This function determines the absolute path to PostgreSQL's locale directory based on the location of the current executable. It uses the  function to compute the path by using the compile-time constants LOCALEDIR and PGBINDIR to establish the relative relationship between the binary directory and the locale directory.

The function is crucial for PostgreSQL's internationalization (i18n) support, enabling the system to locate message translation files and locale-specific data at runtime, particularly in relocatable installations where the actual installation path may differ from the compile-time paths.

## Parameters / Member Variables
- `*my_exec_path`: Input parameter containing the full path to the current executable
- `*ret_path`: Output buffer where the computed locale directory path will be stored (must be at least MAXPGPATH bytes)
## Dependencies
- Functions called/Symbols referenced:
  - [make_relative_path](../m/make_relative_path.md)
  - LOCALEDIR (compile-time constant)
  - PGBINDIR (compile-time constant)
- Called from (representative examples):
  - [pg_bindtextdomain](../p/pg_bindtextdomain.md) (src/backend/utils/init/miscinit.c:1942)
  - [get_configdata](get_configdata.md) (src/common/config_info.c:96)
  - [set_pglocale_pgservice](../s/set_pglocale_pgservice.md) (src/common/exec.c:473)

## Notes and Other Information
- Essential component of PostgreSQL's internationalization infrastructure
- Used to locate message translation files (.po/.mo files) for different languages
- The function assumes ret_path buffer is sufficiently large (MAXPGPATH)
- Part of the path resolution system that enables relocatable PostgreSQL installations
- Called during locale initialization to set up message domains for gettext

## Simplified Source

```c
void get_locale_path(const char *my_exec_path, char *ret_path) {
    // Calculate locale directory path relative to executable location
    make_relative_path(ret_path, LOCALEDIR, PGBINDIR, my_exec_path);
}
```