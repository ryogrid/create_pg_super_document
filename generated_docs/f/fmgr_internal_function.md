# fmgr_internal_function

## Location
[src/backend/utils/fmgr/fmgr.c:595-610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L595-L610)

## Overview
Looks up an internal PostgreSQL function by name and returns its corresponding OID, primarily used for validation purposes.

## Definition

```c
struct fmgr_security_definer_cache
{
	FmgrInfo	flinfo;			/* lookup info for target function */
	Oid			userid;			/* userid to set, or InvalidOid */
	List	   *configNames;	/* GUC names to set, or NIL */
	List	   *configHandles;	/* GUC handles to set, or NIL */
	List	   *configValues;	/* GUC values to set, or NIL */
	Datum		arg;			/* passthrough argument for plugin modules */
};
```
## Detailed Description
The  function provides a specialized lookup mechanism specifically designed for the function manager's internal validator. Given a function name as a string, it searches PostgreSQL's built-in function table to determine if the name corresponds to a valid internal function.

This function serves as a bridge between string-based function names and PostgreSQL's internal OID-based function identification system. It's particularly important for validation scenarios where the system needs to verify that a claimed internal function actually exists in the built-in function catalog.

The function leverages the  function to perform the actual lookup in the internal function table, then extracts the function OID from the resulting FmgrBuiltin structure if found.

## Parameters
- `funcname`: const char pointer to the null-terminated string containing the function name to look up

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_lookupByName](fmgr_lookupByName.md)
  - FmgrBuiltin (structure type)
  - InvalidOid (constant)
- Called from (representative examples):
  - [fmgr_internal_validator](fmgr_internal_validator.md)
  - OidFunctionCall9

## Notes and Other Information
- Returns InvalidOid if the function name is not found in the internal function table
- Specifically designed for use by fmgr_internal_validator for function validation
- Only searches built-in/internal PostgreSQL functions, not user-defined functions
- Part of PostgreSQL's function manager (fmgr) subsystem
- The lookup is case-sensitive and must match exactly with internal function names

## Simplified Source

```c
Oid fmgr_internal_function(const char *proname)
{
    const FmgrBuiltin *builtin = fmgr_lookupByName(proname);

    if (builtin == NULL)
        return InvalidOid;

    return builtin->foid;
}
```