# compile_pltcl_function

## Location
[src/pl/tcl/pltcl.c:1400-1758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L1400-L1758)

## Overview
Compiles or retrieves a cached PL/Tcl function descriptor, handling function metadata extraction, Tcl procedure creation, and proper memory management.

## Definition
```c
static pltcl_proc_desc *
compile_pltcl_function(Oid fn_oid, Oid tgreloid, 
                      bool is_event_trigger, bool pltrusted)
```

## Detailed Description
This function serves as the central compilation and caching mechanism for PL/Tcl functions. It maintains a hash table of compiled function descriptors and handles the complete lifecycle of function preparation, from initial compilation through caching and cache validation.

The function performs several key operations: it first checks if a valid cached version exists by comparing transaction IDs and tuple IDs; if not found or outdated, it extracts function metadata from pg_proc, analyzes argument and return types, creates appropriate memory contexts, generates internal Tcl procedure names, builds the complete Tcl procedure definition with proper argument handling, and finally evaluates the procedure definition in the Tcl interpreter.

For different function types (regular functions, triggers, event triggers), it handles argument processing differently: regular functions get numbered parameters with type conversion setup, triggers receive predefined TG_* variables plus NEW/OLD tuple arrays, and event triggers get TG_event and TG_tag parameters. The function also performs comprehensive error handling with proper resource cleanup on failure.

## Parameters / Member Variables
- `fn_oid`: Object ID of the PostgreSQL function to compile
- `tgreloid`: Object ID of the relation for trigger functions, or InvalidOid for regular functions
- `is_event_trigger`: Boolean flag indicating if this is an event trigger function
- `pltrusted`: Boolean flag indicating if this is a trusted PL/Tcl function (affects interpreter selection)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [hash_search](../h/hash_search.md)
  - HeapTupleHeaderGetRawXmin
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - AllocSetContextCreate
  - [pltcl_fetch_interp](../p/pltcl_fetch_interp.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [getTypeIOParam](../g/getTypeIOParam.md)
  - [type_is_rowtype](../t/type_is_rowtype.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - TextDatumGetCString
  - Tcl_EvalEx
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [pltcl_func_handler](../p/pltcl_func_handler.md)
  - [pltcl_trigger_handler](../p/pltcl_trigger_handler.md)
  - [pltcl_event_trigger_handler](../p/pltcl_event_trigger_handler.md)

## Notes and Other Information
- Uses hash table caching with procedure OID, trigger relation OID, and user ID as composite key
- Validates cached entries using transaction ID (xmin) and tuple ID comparison to detect CREATE OR REPLACE FUNCTION changes
- Creates separate memory context for each function descriptor to enable proper cleanup
- Generates unique internal procedure names using function OID and type suffixes (_trigger, _evttrigger)
- Handles reference counting for function descriptors to enable safe concurrent access and cleanup
- Disallows pseudotype arguments and returns except for specific allowed types (VOID, RECORD, etc.)
- Uses PG_TRY/PG_CATCH/PG_END_TRY for exception safety with proper resource cleanup
- Supports UTF-8 encoding conversion for function source code using UTF_E2U macro
- Creates comprehensive Tcl procedure definitions with upvar statements for global data access
- Implements different argument handling strategies for regular functions vs triggers vs event triggers