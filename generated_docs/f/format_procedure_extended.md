# format_procedure_extended

## Location
src/backend/utils/adt/regproc.c: 326 - 397

## Overview
Core function that converts procedure OIDs to formatted string representation with configurable behavior through flags, supporting both standard and qualified formatting modes.

## Definition
```c
char *format_procedure_extended(Oid procedure_oid, bits16 flags)
```

## Detailed Description
The `format_procedure_extended` function is the foundational procedure formatting function in PostgreSQL. It provides the core implementation for converting procedure OIDs to their string representation in "procedure_name(argument_types)" format, with configurable behavior controlled by flags.

The function performs the following operations:
1. Looks up the procedure in the system catalog (pg_proc) using the provided OID
2. Retrieves procedure metadata including name, namespace, and argument types
3. Determines whether schema qualification is needed based on visibility and flags
4. Constructs the formatted string with procedure name and argument type list
5. Handles error cases based on flag settings

The function supports two primary formatting flags:
- `FORMAT_PROC_INVALID_AS_NULL` (0x01): Returns NULL for invalid/unknown procedures instead of numeric OID
- `FORMAT_PROC_FORCE_QUALIFY` (0x02): Forces schema qualification regardless of search path visibility

## Parameters / Member Variables
- `procedure_oid`: The OID of the procedure to format
- `flags`: Bit flags controlling formatting behavior:
  - `FORMAT_PROC_INVALID_AS_NULL`: Return NULL for invalid procedures
  - `FORMAT_PROC_FORCE_QUALIFY`: Always include schema qualification

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md): System catalog lookup for procedure information
  - `HeapTupleIsValid`: Validates catalog lookup results
  - `Form_pg_proc`: Procedure catalog structure access
  - `IsBootstrapProcessingMode`: Bootstrap mode check (not supported)
  - [FunctionIsVisible](../F/FunctionIsVisible.md): Determines if procedure is visible in current search path
  - [get_namespace_name](../g/get_namespace_name.md): Retrieves schema name for qualification
  - `quote_qualified_identifier`: Properly quotes schema-qualified identifiers
  - [format_type_be](format_type_be.md): Formats argument types
  - [format_type_be_qualified](format_type_be_qualified.md): Formats argument types with schema qualification
  - `initStringInfo`: String buffer initialization
  - [ReleaseSysCache](../R/ReleaseSysCache.md): System catalog cache cleanup
- Called from (representative examples):
  - [format_procedure](format_procedure.md): Simple procedure formatting wrapper
  - [format_procedure_qualified](format_procedure_qualified.md): Schema-qualified procedure formatting wrapper
  - [getObjectDescription](../g/getObjectDescription.md): Object description generation
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md): Object identity component extraction

## Notes and Other Information
- Returns a palloc'd string that must be managed within PostgreSQL's memory context
- Does not support bootstrap mode operation (assertion will fail)
- For invalid procedures, behavior depends on flags: returns NULL or numeric OID string
- Uses PostgreSQL's string buffer system (StringInfo) for efficient string construction
- Handles argument type formatting with optional schema qualification
- Central implementation for all procedure formatting in the PostgreSQL backend
- Provides foundation for both user-facing and internal procedure name formatting
- Schema qualification logic considers both search path visibility and explicit flag requirements