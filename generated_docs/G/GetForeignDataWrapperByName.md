# GetForeignDataWrapperByName

## Location
src/backend/foreign/foreign.c: 96 - 110

## Overview
Retrieves a foreign-data wrapper object by its name, providing a name-based lookup interface with optional error handling for missing wrappers.

## Definition
```c
ForeignDataWrapper *GetForeignDataWrapperByName(const char *fdwname, bool missing_ok)
```

## Detailed Description
GetForeignDataWrapperByName provides a convenient interface to look up foreign-data wrapper objects by their textual name rather than by OID. The function first resolves the wrapper name to its corresponding OID using get_foreign_data_wrapper_oid, then delegates to GetForeignDataWrapper for the actual object retrieval. This two-step process allows for name-based lookups while reusing the existing OID-based infrastructure. The missing_ok parameter controls error handling behavior - when true, the function returns NULL if the named wrapper doesn't exist; when false, an error is raised.

## Parameters / Member Variables
- `fdwname`: The name of the foreign-data wrapper to retrieve (null-terminated string)
- `missing_ok`: Boolean flag controlling error handling - if true, returns NULL for missing FDWs instead of raising an error

## Dependencies
- Functions called/Symbols referenced:
  - get_foreign_data_wrapper_oid (name to OID resolution)
  - OidIsValid (OID validation macro)
  - GetForeignDataWrapper (OID-based FDW retrieval)
  - ForeignDataWrapper (return type structure)
- Called from (representative examples):
  - CreateForeignDataWrapper
  - CreateForeignServer

## Notes and Other Information
- Located in src/backend/foreign/foreign.c:96-110
- Provides a name-based interface to the OID-based FDW lookup system
- Returns NULL if the wrapper name cannot be resolved to a valid OID and missing_ok is true
- The returned ForeignDataWrapper structure is palloc'd and should be freed by the caller
- This function is commonly used during DDL operations where FDWs are referenced by name
- Error handling behavior is controlled by the missing_ok parameter, similar to other PostgreSQL catalog lookup functions