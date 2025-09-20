# get_available_versions_for_extension

## Location
[src/backend/commands/extension.c:2146-2259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2146-L2259)

## Overview
This static function is the core logic that discovers all available versions for a specific extension and populates the result tuplestore with detailed version information.

## Definition

```c
struct dirent *de;
```
## Detailed Description
This function implements the inner loop logic for pg_available_extension_versions. Given an extension's primary control file, it discovers all installable and non-installable versions by examining the extension's script directory and version update graph. 

The function works in two phases:
1. **Direct installable versions**: For each version that can be directly installed, it reads the version-specific control file and adds a row with complete version information (name, version, superuser, trusted, relocatable, schema, requires, comment).

2. **Update-reachable versions**: For non-directly-installable versions that can be reached via update paths from installable versions, it determines the installation path and adds rows for these versions as well.

The function returns an 8-column result set for each available version:
- Extension name
- Version string  
- Superuser required flag
- Trusted flag
- Relocatable flag
- Schema name (nullable)
- Required extensions array (nullable)
- Comment/description (nullable)

## Parameters / Member Variables
- : Pointer to the primary ExtensionControlFile containing base extension metadata
- : Tuplestorestate where result rows will be stored
- : TupleDesc describing the structure of result rows

## Dependencies
- Functions called/Symbols referenced:
  - [get_ext_ver_list](get_ext_ver_list.md)
  - [read_extension_aux_control_file](../r/read_extension_aux_control_file.md)
  - DirectFunctionCall1
  - namein
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - CStringGetTextDatum
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - [convert_requires_to_datum](../c/convert_requires_to_datum.md)
  - tuplestore_putvalues
  - [find_install_path](../f/find_install_path.md)
- Called from (representative examples):
  - [pg_available_extension_versions](../p/pg_available_extension_versions.md)

## Notes and Other Information
- This is a static (internal) function used exclusively by pg_available_extension_versions
- The function handles both directly installable versions and versions reachable through update paths
- Version-specific control files are read using read_extension_aux_control_file to get precise parameters for each version
- The function uses PostgreSQL's extension version graph analysis to determine installation paths
- Non-installable versions inherit certain parameters (name, schema, comment) from their installation root while getting version-specific parameters (superuser, trusted, relocatable, requires) from their own control files
- The requires field is converted to a PostgreSQL array datum using convert_requires_to_datum