# CheckFunctionValidatorAccess

## Location
src/backend/utils/fmgr/fmgr.c: 2145 - 2200

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
  - SearchSysCache1 (system catalog lookups)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (extract structure from heap tuple)
  - object_aclcheck (access control checking)
  - aclcheck_error (access control error reporting)
  - GetUserId (current user identification)
  - ReleaseSysCache (cache cleanup)
  - ereport/errmsg/errcode (error reporting)
  - Form_pg_proc, Form_pg_language (catalog structure types)
  - AclResult, ACL_USAGE, ACL_EXECUTE (access control types/constants)
  - OBJECT_LANGUAGE, OBJECT_FUNCTION (object type constants)
- Called from (representative examples):
  - fmgr_internal_validator (internal function validator)
  - fmgr_c_validator (C language validator)
  - fmgr_sql_validator (SQL language validator)
  - plperl_validator (Perl language validator)
  - plpython3_validator (Python language validator)

## Notes and Other Information
- Located in src/backend/utils/fmgr/fmgr.c:2145-2200
- Currently always returns true; false return value is reserved for future expansion
- Essential security function for all procedural language implementations
- Throws user-facing errors for invalid OIDs since validators can be called with user-specified parameters
- The function includes extensive comments explaining the security rationale and threat model
- Validators should call this function before performing any substantial validation work
- When this function would return false (future expansion), callers should skip validation and call PG_RETURN_VOID()
- Part of the procedural language implementation support routines in PostgreSQL's function manager