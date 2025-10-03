# get_configdata

## Location
[src/common/config_info.c:33-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/config_info.c#L33-L201)

## Overview
Returns PostgreSQL's configure-time constants and installation paths in a structured format for use by various PostgreSQL utilities and applications.

## Definition

```c
ConfigData *
get_configdata(const char *my_exec_path, size_t *configdata_len)
```
## Detailed Description
The  function generates an array of configuration data containing PostgreSQL's build-time constants and computed installation paths. It returns 23 key-value pairs that include directory paths (BINDIR, LIBDIR, etc.), compiler information (CC, CFLAGS, etc.), and other configure-time settings.

The function computes most directory paths dynamically based on the provided executable path, ensuring the returned paths are correct for the actual PostgreSQL installation location. This is essential for portable installations and development environments where PostgreSQL may not be installed in the default system locations.

Build-time compiler and linker information is retrieved from preprocessor macros (VAL_CC, VAL_CFLAGS, etc.) if available, or marked as "not recorded" if the information was not captured during compilation.

## Parameters / Member Variables
- `*my_exec_path`: The path to the current PostgreSQL executable, used as a reference point for computing other installation paths
- `*configdata_len`: Output parameter that receives the number of ConfigData items in the returned array (always set to 23)
## Dependencies
- Functions called/Symbols referenced:
  - palloc_array
  - [pstrdup](../p/pstrdup.md)
  - [strlcpy](../s/strlcpy.md)
  - [strlcat](../s/strlcat.md)
  - strrchr
  - [cleanup_path](../c/cleanup_path.md)
  - [get_doc_path](get_doc_path.md)
  - [get_html_path](get_html_path.md)
  - [get_include_path](get_include_path.md)
  - [get_pkginclude_path](get_pkginclude_path.md)
  - [get_includeserver_path](get_includeserver_path.md)
  - [get_lib_path](get_lib_path.md)
  - [get_pkglib_path](get_pkglib_path.md)
  - [get_locale_path](get_locale_path.md)
  - [get_man_path](get_man_path.md)
  - [get_share_path](get_share_path.md)
  - [get_etc_path](get_etc_path.md)
  - [ConfigData](../C/ConfigData.md) (struct)

- Called from (representative examples):
  - [pg_config](../p/pg_config.md) (backend function)
  - [main](../m/main.md) (in pg_config utility)

## Notes and Other Information
- Returns exactly 23 ConfigData entries covering all major PostgreSQL installation paths and build settings
- The caller is responsible for freeing the returned ConfigData array using pfree
- Directory paths are computed relative to the executable path, making the function suitable for relocated installations
- Includes special handling for PGXS path construction (adds "/pgxs/src/makefiles/pgxs.mk" to PKGLIBDIR)
- Build-time variables (CC, CFLAGS, etc.) may show "not recorded" if not captured during compilation
- The function is used primarily by the pg_config utility and backend configuration reporting functions