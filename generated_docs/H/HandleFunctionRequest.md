# HandleFunctionRequest

## Location
[src/backend/tcop/fastpath.c:189-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/fastpath.c#L189-L329)

## Overview
Handles fast-path function calls from the frontend client, serving as the server-side implementation of the PQfn protocol for direct function invocation without SQL parsing.

## Definition
void HandleFunctionRequest(StringInfo msgBuf)

## Detailed Description
HandleFunctionRequest is the main entry point for PostgreSQL's fast-path function call protocol. It processes function call requests that bypass the normal SQL parser and planner, allowing clients to directly invoke PostgreSQL functions by OID. The function performs comprehensive validation including transaction state checks, permission verification for both schema and function access, and argument parsing. It manages transaction snapshots, handles strict function semantics (where NULL arguments prevent execution), invokes the target function, and sends the result back to the client. The function also supports logging and duration tracking for performance monitoring. Memory management is handled automatically by the MessageContext.

## Parameters / Member Variables
- : StringInfo containing the parsed message buffer with function call details from the client

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO
  - FUNC_MAX_ARGS
  - [AclResult](../A/AclResult.md)
  - [fp_info](../f/fp_info.md)
  - [IsAbortedTransactionBlockState](../I/IsAbortedTransactionBlockState.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [fetch_fp_info](../f/fetch_fp_info.md)
  - LOGSTMT_ALL
  - [object_aclcheck](../o/object_aclcheck.md)
  - ACL_USAGE
  - ACL_EXECUTE
  - [aclcheck_error](../a/aclcheck_error.md)
  - OBJECT_SCHEMA
  - OBJECT_FUNCTION
  - [get_namespace_name](../g/get_namespace_name.md)
  - [get_func_name](../g/get_func_name.md)
  - InvokeNamespaceSearchHook
  - InvokeFunctionExecuteHook
  - InitFunctionCallInfoData
  - [parse_fcall_arguments](../p/parse_fcall_arguments.md)
  - [pq_getmsgend](../p/pq_getmsgend.md)
  - FunctionCallInvoke
  - [SendFunctionResult](../S/SendFunctionResult.md)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [check_log_duration](../c/check_log_duration.md)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md)

## Notes and Other Information
- This function corresponds to the libpq protocol symbol "F" for fast-path function calls
- Memory allocations are automatically cleaned up by the MessageContext when control returns to PostgresMain
- The function enforces strict function semantics - if any argument is NULL and the function is marked strict, the function is not called
- Permission checking includes both schema usage rights and function execution rights
- Transaction snapshots are managed to ensure consistent data access during function execution
- Supports comprehensive logging including statement logging and duration tracking
- The fast-path interface cannot handle collation-sensitive functions (uses InvalidOid for collation)
- Caching of function lookup information was removed in favor of fresh lookups on every call

## Simplified Source

```c
// Simplified version of HandleFunctionRequest
void HandleFunctionRequest(StringInfo msgBuf) {
    LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
    Oid fid;
    AclResult aclresult;
    int16 rformat;
    Datum retval;
    struct fp_info my_fp;
    struct fp_info *fip;
    bool callit;
    bool was_logged = false;
    char msec_str[32];

    // Check transaction state - reject if in aborted transaction
    if (IsAbortedTransactionBlockState())
        ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                       errmsg("current transaction is aborted")));

    // Set up transaction snapshot for consistent data access
    PushActiveSnapshot(GetTransactionSnapshot());

    // Parse function OID from message buffer
    fid = (Oid) pq_getmsgint(msgBuf, 4);

    // Look up function information
    fip = &my_fp;
    fetch_fp_info(fid, fip);

    // Log function call if statement logging is enabled
    if (log_statement == LOGSTMT_ALL) {
        ereport(LOG, (errmsg("fastpath function call: \"%s\" (OID %u)",
                            fip->fname, fid)));
        was_logged = true;
    }

    // Check schema usage permission
    aclresult = object_aclcheck(NamespaceRelationId, fip->namespace,
                               GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_SCHEMA,
                      get_namespace_name(fip->namespace));
    InvokeNamespaceSearchHook(fip->namespace, true);

    // Check function execution permission
    aclresult = object_aclcheck(ProcedureRelationId, fid,
                               GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(fid));
    InvokeFunctionExecuteHook(fid);

    // Initialize function call info and parse arguments
    InitFunctionCallInfoData(*fcinfo, &fip->flinfo, 0, InvalidOid, NULL, NULL);
    rformat = parse_fcall_arguments(msgBuf, fip, fcinfo);
    pq_getmsgend(msgBuf);

    // Handle strict functions - don't call if any argument is NULL
    callit = true;
    if (fip->flinfo.fn_strict) {
        for (int i = 0; i < fcinfo->nargs; i++) {
            if (fcinfo->args[i].isnull) {
                callit = false;
                break;
            }
        }
    }

    // Execute function or return NULL for strict functions with NULL args
    if (callit) {
        retval = FunctionCallInvoke(fcinfo);
    } else {
        fcinfo->isnull = true;
        retval = (Datum) 0;
    }

    // Check for interrupts and send result to client
    CHECK_FOR_INTERRUPTS();
    SendFunctionResult(retval, fcinfo->isnull, fip->rettype, rformat);

    // Clean up snapshot
    PopActiveSnapshot();

    // Log duration if enabled
    switch (check_log_duration(msec_str, was_logged)) {
        case 1:
            ereport(LOG, (errmsg("duration: %s ms", msec_str)));
            break;
        case 2:
            ereport(LOG, (errmsg("duration: %s ms  fastpath function call: \"%s\" (OID %u)",
                                msec_str, fip->fname, fid)));
            break;
    }
}
```

Key simplifications made:
- Removed detailed error message text for brevity
- Consolidated variable declarations at the top
- Added clear comments for each major logical step
- Simplified the strict function checking loop
- Focused on the main execution flow
- Abstracted complex error handling details
- Made the permission checking flow more readable