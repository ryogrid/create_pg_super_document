# fmgr_info_cxt_security

## Location
src/backend/utils/fmgr/fmgr.c: 147 - 280

## Overview
The core function that initializes FmgrInfo structs by looking up function metadata, handling both builtin and catalog-defined functions with optional security enforcement.

## Definition
```c
static void fmgr_info_cxt_security(Oid functionId, FmgrInfo *finfo, MemoryContext mcxt, bool ignore_security)
```

## Detailed Description
fmgr_info_cxt_security is the workhorse function behind all FmgrInfo initialization in PostgreSQL. It performs a comprehensive lookup of function metadata and properly configures the FmgrInfo struct for efficient function calls. The function implements a two-tier lookup strategy: first checking if the function is a builtin (using fmgr_isbuiltin for O(1) lookup), then falling back to catalog lookup via pg_proc for user-defined and other functions.

The function handles multiple function languages (INTERNAL, C, SQL, and procedural languages) and implements security policies including SECURITY DEFINER functions and function hooks. When security features are needed, it delegates to fmgr_security_definer rather than setting up direct function calls. The function also manages function statistics tracking policies based on the function type and language.

## Parameters / Member Variables
- `functionId`: The Oid of the function to look up and initialize
- `finfo`: Pointer to the FmgrInfo struct to be initialized with function metadata
- `mcxt`: Memory context for allocating subsidiary data associated with this FmgrInfo
- `ignore_security`: Boolean flag to bypass security checks, used to prevent recursion in security-related functions

## Dependencies
- Functions called/Symbols referenced:
  - fmgr_isbuiltin (fast builtin function lookup)
  - SearchSysCache1, ReleaseSysCache (catalog access for pg_proc entries)
  - heap_attisnull (checking for non-null proconfig values)
  - FmgrHookIsNeeded (checking if function hooks are required)
  - fmgr_security_definer (security definer call handler)
  - fmgr_lookupByName (builtin function lookup by name for aliased internals)
  - fmgr_info_C_lang (C language function setup)
  - fmgr_sql (SQL language function handler)
  - fmgr_info_other_lang (procedural language function setup)
  - Various constants: TRACK_FUNC_ALL, TRACK_FUNC_PL, TRACK_FUNC_OFF
- Called from (representative examples):
  - fmgr_info (convenience wrapper using current memory context)
  - fmgr_info_cxt (wrapper with explicit memory context)
  - fmgr_info_other_lang (recursive call for procedural language setup)
  - fmgr_security_definer (recursive call with security bypass)

## Notes and Other Information
- This is a static function, only accessible within the fmgr.c file
- Implements the core logic for all function call setup in PostgreSQL
- Critical for security: properly handles SECURITY DEFINER functions and function hooks
- Optimized for builtin functions with fast-path lookup avoiding catalog access
- Handles function aliasing where user-created functions point to builtin internals
- Sets appropriate statistics tracking policies based on function language and type
- The fn_oid field is set last to ensure struct validity in case of errors
- Part of PostgreSQL's Function Manager (fmgr) subsystem responsible for function call dispatch
- Manages memory allocation through the specified memory context for long-term function info storage