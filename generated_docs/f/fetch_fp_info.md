# fetch_fp_info

## Location
[src/backend/tcop/fastpath.c:120-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/fastpath.c#L120-L188)

## Overview
Performs catalog lookups to load function information into a fp_info structure for a given function OID, enabling safe invocation through the PostgreSQL fast-path interface.

## Definition
static void fetch_fp_info(Oid func_id, struct fp_info *fip)

## Detailed Description
fetch_fp_info is responsible for safely retrieving and validating function metadata from the system catalog (pg_proc) and populating a fp_info structure. The function performs several critical validations: it ensures the function exists, verifies it's a regular function (not a procedure or aggregate), confirms it doesn't return sets, and checks that the argument count doesn't exceed FUNC_MAX_ARGS. The function extracts essential metadata including namespace, return type, argument types, and function name. It also initializes the function manager info (flinfo) for efficient function calls. The funcid field is set last as a validity marker, ensuring the structure is only considered valid when fully populated.

## Parameters / Member Variables
- : OID of the function to look up in the system catalog
- : Pointer to fp_info structure to be populated with function information

## Dependencies
- Functions called/Symbols referenced:
  - [fp_info](fp_info.md)
  - Form_pg_proc
  - MemSet
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - PROKIND_FUNCTION
  - FUNC_MAX_ARGS
  - NAMEDATALEN
  - strlcpy
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [fmgr_info](fmgr_info.md)
- Called from (representative examples):
  - [HandleFunctionRequest](../H/HandleFunctionRequest.md)

## Notes and Other Information
- This is a static function used internally within the fast-path protocol implementation
- The function performs comprehensive validation to ensure only safe functions can be called via fast-path
- Memory is zeroed at the start and funcid is set to InvalidOid as a safety measure
- The funcid field serves as a validity indicator - it's only set to the correct value when the structure is fully populated
- Functions that return sets or are not regular functions (procedures, aggregates) are rejected
- The function handles memory management by releasing the system cache tuple after extracting needed information