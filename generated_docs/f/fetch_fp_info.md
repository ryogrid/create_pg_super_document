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
  - [strlcpy](../s/strlcpy.md)
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

## Simplified Source

```c
// Simplified version of fetch_fp_info
static void fetch_fp_info(Oid func_id, struct fp_info *fip) {
    HeapTuple func_htp;
    Form_pg_proc pp;

    // Initialize structure safely - clear all fields and mark as invalid
    MemSet(fip, 0, sizeof(struct fp_info));
    fip->funcid = InvalidOid;

    // Look up function in system catalog
    func_htp = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_id));
    if (!HeapTupleIsValid(func_htp)) {
        ereport(ERROR, "function with OID %u does not exist", func_id);
    }
    pp = (Form_pg_proc) GETSTRUCT(func_htp);

    // Validate function is safe for fastpath interface
    if (pp->prokind != PROKIND_FUNCTION || pp->proretset) {
        ereport(ERROR, "cannot call function via fastpath interface");
    }

    // Check argument count limit
    if (pp->pronargs > FUNC_MAX_ARGS) {
        elog(ERROR, "function has too many arguments");
    }

    // Extract function metadata
    fip->namespace = pp->pronamespace;
    fip->rettype = pp->prorettype;
    memcpy(fip->argtypes, pp->proargtypes.values, pp->pronargs * sizeof(Oid));
    strlcpy(fip->fname, NameStr(pp->proname), NAMEDATALEN);

    // Clean up catalog access
    ReleaseSysCache(func_htp);

    // Initialize function manager info for efficient calls
    fmgr_info(func_id, &fip->flinfo);

    // Mark structure as valid (this must be last!)
    fip->funcid = func_id;
}
```

Key simplifications made:
- Removed detailed error message formatting for clarity
- Simplified error condition descriptions while preserving validation logic
- Abstracted complex catalog lookup details with descriptive comments
- Focused on the main execution path: validate → extract → initialize
- Preserved the critical safety pattern of setting funcid last