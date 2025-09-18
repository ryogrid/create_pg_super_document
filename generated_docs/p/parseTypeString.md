# parseTypeString

## Location
src/backend/parser/parse_type.c: 785 - 821

## Overview
Parses a SQL-compatible type declaration string and converts it to a type OID and type modifier, providing a complete type resolution solution.

## Definition
```c
bool parseTypeString(const char *str, Oid *typeid_p, int32 *typmod_p, Node *escontext)
```

## Detailed Description
This function provides a complete pipeline for converting a string representation of a SQL type into its internal PostgreSQL representation. It combines string parsing with type lookup and validation to produce the final type OID and type modifier.

The function performs several key operations:
1. Parses the type string into a TypeName node using typeStringToTypeName()
2. Looks up the actual type in the system catalog using LookupTypeName()
3. Validates that the type is fully defined (not just a shell type)
4. Returns the type OID and modifier through output parameters

The function supports soft error handling when escontext is provided, allowing callers to handle type resolution failures gracefully rather than having errors thrown.

Shell types (types that are declared but not fully defined) are explicitly rejected to ensure only usable types are returned.

## Parameters / Member Variables
- `str`: The string containing the SQL type declaration to parse and resolve
- `typeid_p`: Output parameter to receive the resolved type OID
- `typmod_p`: Output parameter to receive the type modifier (e.g., length for varchar)
- `escontext`: Error context node for soft error handling; if NULL, errors are thrown normally

## Dependencies
- Functions called/Symbols referenced:
  - typeStringToTypeName
  - LookupTypeName
  - ErrorSaveContext
  - TypeNameToString
  - Form_pg_type
  - GETSTRUCT
  - ReleaseSysCache
  - ereturn
- Called from (representative examples):
  - pg_input_is_valid_common (src/backend/utils/adt/misc.c:799)
  - regtypein (src/backend/utils/adt/regproc.c:1198)
  - to_regtypemod (src/backend/utils/adt/regproc.c:1237)
  - parseNameAndArgTypes (src/backend/utils/adt/regproc.c:2023)
  - plperl_spi_prepare (src/pl/plperl/plperl.c:3630)
  - PLy_spi_prepare (src/pl/plpython/plpy_spi.c:108)
  - pltcl_SPI_prepare (src/pl/tcl/pltcl.c:2616)

## Notes and Other Information
- Returns true on success, false on failure (when escontext is provided)
- Throws ERROR on failure when escontext is NULL
- Rejects shell types (types that exist but are not fully defined)
- Properly manages system cache references with ReleaseSysCache()
- Used extensively by procedural languages for type resolution
- Commonly used in input validation functions and type registration functions
- The soft error handling makes it suitable for user-facing functions that need to validate type names