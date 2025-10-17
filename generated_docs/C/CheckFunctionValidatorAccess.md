# CheckFunctionValidatorAccess

## Location
[src/backend/utils/fmgr/fmgr.c:2145-2200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L2145-L2200)

## Overview
Verifies that a validator function is properly associated with a procedural language and that the user has appropriate access privileges to both the language and the function being validated.

## Definition
```c
bool CheckFunctionValidatorAccess(Oid validatorOid, Oid functionOid)
```

## Detailed Description
This function implements a comprehensive security check for procedural language validators in PostgreSQL. It ensures that validator functions can only be called in legitimate contexts by verifying:

1. **Validator-Language Association**: Confirms the validator corresponds to the function's language, preventing unauthorized validation attempts
2. **Language Usage Privileges**: Checks that the user has USAGE privilege on the procedural language
3. **Function Execution Privileges**: Verifies the user has EXECUTE privilege on the function being validated

This security model ensures that untrusted language validators can safely assume they process only superuser-chosen source code, and prevents users from achieving through explicit validator calls what they couldn't achieve through CREATE FUNCTION or normal function execution.

The function performs comprehensive privilege checking to prevent potential security vulnerabilities, particularly for users with limited database privileges (no TEMP rights, no permanent schema CREATE rights).

## Parameters / Member Variables
- `validatorOid`: OID of the validator function being called
- `functionOid`: OID of the function to be validated

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookups)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (extract structure from heap tuple)
  - [object_aclcheck](../o/object_aclcheck.md) (access control checking)
  - [aclcheck_error](../a/aclcheck_error.md) (access control error reporting)
  - [GetUserId](../G/GetUserId.md) (current user identification)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - ereport/errmsg/errcode (error reporting)
  - Form_pg_proc, Form_pg_language (catalog structure types)
  - [AclResult](../A/AclResult.md), ACL_USAGE, ACL_EXECUTE (access control types/constants)
  - OBJECT_LANGUAGE, OBJECT_FUNCTION (object type constants)
- Called from (representative examples):
  - [fmgr_internal_validator](../f/fmgr_internal_validator.md) (internal function validator)
  - [fmgr_c_validator](../f/fmgr_c_validator.md) (C language validator)
  - [fmgr_sql_validator](../f/fmgr_sql_validator.md) (SQL language validator)
  - [plperl_validator](../p/plperl_validator.md) (Perl language validator)
  - [plpython3_validator](../p/plpython3_validator.md) (Python language validator)

## Notes and Other Information
- Located in src/backend/utils/fmgr/fmgr.c:2145-2200
- Currently always returns true; false return value is reserved for future expansion
- Essential security function for all procedural language implementations
- Throws user-facing errors for invalid OIDs since validators can be called with user-specified parameters
- The function includes extensive comments explaining the security rationale and threat model
- Validators should call this function before performing any substantial validation work
- When this function would return false (future expansion), callers should skip validation and call PG_RETURN_VOID()
- Part of the procedural language implementation support routines in PostgreSQL's function manager

## Simplified Source

```c
bool CheckFunctionValidatorAccess(Oid validatorOid, Oid functionOid) {
    HeapTuple procTup, langTup;
    Form_pg_proc procStruct;
    Form_pg_language langStruct;
    AclResult aclresult;

    // Get function catalog entry
    procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
    if (!HeapTupleIsValid(procTup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("function with OID %u does not exist", functionOid)));
    procStruct = (Form_pg_proc) GETSTRUCT(procTup);

    // Get language catalog entry
    langTup = SearchSysCache1(LANGOID, ObjectIdGetDatum(procStruct->prolang));
    if (!HeapTupleIsValid(langTup))
        elog(ERROR, "cache lookup failed for language %u", procStruct->prolang);
    langStruct = (Form_pg_language) GETSTRUCT(langTup);

    // Verify validator matches language
    if (langStruct->lanvalidator != validatorOid)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("language validation function %u called for language %u instead of %u",
                       validatorOid, procStruct->prolang, langStruct->lanvalidator)));

    // Check language usage privilege
    aclresult = object_aclcheck(LanguageRelationId, procStruct->prolang,
                                GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_LANGUAGE, NameStr(langStruct->lanname));

    // Check function execution privilege
    aclresult = object_aclcheck(ProcedureRelationId, functionOid,
                                GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_FUNCTION, NameStr(procStruct->proname));

    ReleaseSysCache(procTup);
    ReleaseSysCache(langTup);

    return true;
}
```