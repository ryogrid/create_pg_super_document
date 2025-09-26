# SPI_getnspname

## Location
[src/backend/executor/spi.c:1332-1337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1332-L1337)

## Overview
Retrieves the namespace (schema) name of a relation as a dynamically allocated string.

## Definition
```c
char *SPI_getnspname(Relation rel)
```

## Detailed Description
SPI_getnspname extracts the namespace (schema) name for a given relation by first obtaining the namespace OID from the relation and then resolving it to its string name. This function provides a convenient way to determine which schema a table, view, or other relation belongs to, which is essential for fully qualified naming and schema-aware operations.

The function combines RelationGetNamespace (to get the namespace OID) with get_namespace_name (to resolve the OID to a name string). The returned string is allocated by get_namespace_name and should be freed by the caller when no longer needed.

## Parameters / Member Variables
- `rel`: Relation structure from which to extract the namespace name

## Dependencies
- Functions called/Symbols referenced:
  - get_namespace_name
  - RelationGetNamespace
- Called from (representative examples):
  - plperl_trigger_build_args (src/pl/plperl/plperl.c)
  - PLy_trigger_build_args (src/pl/plpython/plpy_exec.c)
  - pltcl_trigger_handler (src/pl/tcl/pltcl.c)
  - plsample_trigger_handler (src/test/modules/plsample/plsample.c)

## Notes and Other Information
- Returns a dynamically allocated string that must be freed by the caller
- Does not perform validation on the input Relation pointer
- Commonly used alongside SPI_getrelname to build fully qualified relation names
- Used primarily in procedural language implementations for trigger context information
- The function delegates namespace name resolution to get_namespace_name, which handles cache lookups
- No error handling - assumes valid Relation input and existing namespace
- Does not set any global SPI_result status