# ginarrayextract_2args

## Location
src/backend/access/gin/ginarrayproc.c: 68 - 78

## Overview
A compatibility wrapper function that provides backward compatibility for the old 2-argument version of ginarrayextract, supporting pre-9.1 contrib/intarray opclass declarations.

## Definition
```c
Datum ginarrayextract_2args(PG_FUNCTION_ARGS)
```

## Detailed Description
The `ginarrayextract_2args` function serves as a backward compatibility layer for PostgreSQL installations that need to support pre-9.1 contrib/intarray operator class declarations. Originally, ginarrayextract accepted only two arguments, but it was later extended to accept three arguments. This wrapper function ensures that older pg_proc entries continue to work during database upgrades or when reloading old operator class definitions.

The function performs a simple argument count check and then delegates all processing to the main ginarrayextract function. This design maintains API compatibility while allowing the underlying implementation to evolve.

## Parameters / Member Variables
- Function uses the standard PG_FUNCTION_ARGS interface
- No specific parameters documented as this is a compatibility wrapper
- Internally validates argument count before delegation

## Dependencies  
- Functions called/Symbols referenced:
  - PG_NARGS (macro to get number of function arguments)
  - [ginarrayextract](ginarrayextract.md) (delegates all actual processing to this function)
- Called from:
  - No direct references found (used through pg_proc entries for compatibility)

## Notes and Other Information
- This is a temporary compatibility function intended to be removed eventually
- Required for supporting database upgrades from PostgreSQL versions prior to 9.1
- Specifically supports reloading of pre-9.1 contrib/intarray opclass declarations
- The function includes error checking to ensure proper argument count
- Part of PostgreSQL's backward compatibility infrastructure for GIN array operations