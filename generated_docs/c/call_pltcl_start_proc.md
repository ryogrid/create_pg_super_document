# call_pltcl_start_proc

## Location
[src/pl/tcl/pltcl.c:593-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L593-L679)

## Overview
Calls a user-defined initialization procedure during PL/Tcl interpreter setup, providing customizable initialization logic for PL/Tcl environments.

## Definition

```c
static void
call_pltcl_start_proc(Oid prolang, bool pltrusted)
```
## Detailed Description
The `call_pltcl_start_proc` function is responsible for executing a user-defined initialization procedure when a new PL/Tcl interpreter is created. This mechanism allows database administrators and users to customize the initialization of PL/Tcl environments by specifying a startup function through the `pltcl.start_proc` or `pltclu.start_proc` GUC parameters.

The function performs comprehensive validation of the startup procedure including:
1. Permission checks to ensure the current user can execute the function
2. Language validation to ensure the startup function is written in the same PL/Tcl variant
3. Security validation to prevent SECURITY DEFINER functions (which would change execution context)

The function uses PostgreSQL's standard function call mechanism, ensuring proper integration with statistics collection and execution hooks. Error handling includes a specialized error context callback to provide more helpful error messages.

## Parameters / Member Variables
- `prolang`: OID of the procedural language (pltcl or pltclu) that must match the startup function's language
- `pltrusted`: Boolean indicating whether this is for trusted (true) or untrusted (false) PL/Tcl, determining which GUC parameter to check

## Dependencies
- Functions called/Symbols referenced:
  - [stringToQualifiedNameList](../s/stringToQualifiedNameList.md) (parses function name)
  - [LookupFuncName](../L/LookupFuncName.md) (resolves function OID)
  - [object_aclcheck](../o/object_aclcheck.md) (checks execution permissions) 
  - [SearchSysCache1](../S/SearchSysCache1.md) (retrieves function metadata)
  - [fmgr_info](../f/fmgr_info.md) and `FunctionCallInvoke` (function call mechanism)
  - [start_proc_error_callback](../s/start_proc_error_callback.md) (error context callback)
  - Various PostgreSQL system functions for validation and execution
- Called from (representative examples):
  - [pltcl_init_interp](../p/pltcl_init_interp.md) (during interpreter initialization)

## Notes and Other Information
- Uses GUC parameters `pltcl.start_proc` (trusted) or `pltclu.start_proc` (untrusted) to determine which function to call
- Implements comprehensive security checks: ACL permissions, language matching, and SECURITY DEFINER restrictions
- Uses PostgreSQL's standard function call infrastructure for proper integration with monitoring and hooks
- Provides enhanced error context through `start_proc_error_callback` for better diagnostics
- The function is static, indicating it's only used within the pltcl.c module
- If no startup procedure is configured (NULL or empty string), the function returns immediately without action

## Simplified Source

```c
static void
call_pltcl_start_proc(Oid prolang, bool pltrusted)
{
    char *start_proc;
    const char *gucname;

    // Select appropriate GUC parameter based on trusted/untrusted
    start_proc = pltrusted ? pltcl_start_proc : pltclu_start_proc;
    gucname = pltrusted ? "pltcl.start_proc" : "pltclu.start_proc";

    // Return early if no start procedure configured
    if (start_proc == NULL || start_proc[0] == '\0')
        return;

    // Set up error context for better error messages
    ErrorContextCallback errcallback;
    errcallback.callback = start_proc_error_callback;
    errcallback.arg = unconstify(char *, gucname);
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Parse function name and look it up
    List *namelist = stringToQualifiedNameList(start_proc, NULL);
    Oid procOid = LookupFuncName(namelist, 0, NULL, false);

    // Check user has permission to execute function
    AclResult aclresult = object_aclcheck(ProcedureRelationId, procOid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_FUNCTION, start_proc);

    // Validate function properties
    HeapTuple procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(procOid));
    if (!HeapTupleIsValid(procTup))
        elog(ERROR, "cache lookup failed for function %u", procOid);

    Form_pg_proc procStruct = (Form_pg_proc) GETSTRUCT(procTup);

    // Function must be same language
    if (procStruct->prolang != prolang)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("function \"%s\" is in the wrong language", start_proc)));

    // Function must not be SECURITY DEFINER
    if (procStruct->prosecdef)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("function \"%s\" must not be SECURITY DEFINER", start_proc)));

    ReleaseSysCache(procTup);

    // Call the function using standard SQL function call mechanism
    LOCAL_FCINFO(fcinfo, 0);
    FmgrInfo finfo;
    PgStat_FunctionCallUsage fcusage;

    InvokeFunctionExecuteHook(procOid);
    fmgr_info(procOid, &finfo);
    InitFunctionCallInfoData(*fcinfo, &finfo, 0, InvalidOid, NULL, NULL);
    pgstat_init_function_usage(fcinfo, &fcusage);
    (void) FunctionCallInvoke(fcinfo);
    pgstat_end_function_usage(&fcusage, true);

    // Restore error context
    error_context_stack = errcallback.previous;
}
```