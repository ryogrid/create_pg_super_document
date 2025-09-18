# HandleFunctionRequest

## Location
src/backend/tcop/fastpath.c: 189 - 329

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
  - AclResult
  - fp_info
  - IsAbortedTransactionBlockState
  - GetTransactionSnapshot
  - PushActiveSnapshot
  - pq_getmsgint
  - fetch_fp_info
  - LOGSTMT_ALL
  - object_aclcheck
  - ACL_USAGE
  - ACL_EXECUTE
  - aclcheck_error
  - OBJECT_SCHEMA
  - OBJECT_FUNCTION
  - get_namespace_name
  - get_func_name
  - InvokeNamespaceSearchHook
  - InvokeFunctionExecuteHook
  - InitFunctionCallInfoData
  - parse_fcall_arguments
  - pq_getmsgend
  - FunctionCallInvoke
  - SendFunctionResult
  - PopActiveSnapshot
  - check_log_duration
- Called from (representative examples):
  - PostgresMain

## Notes and Other Information
- This function corresponds to the libpq protocol symbol "F" for fast-path function calls
- Memory allocations are automatically cleaned up by the MessageContext when control returns to PostgresMain
- The function enforces strict function semantics - if any argument is NULL and the function is marked strict, the function is not called
- Permission checking includes both schema usage rights and function execution rights
- Transaction snapshots are managed to ensure consistent data access during function execution
- Supports comprehensive logging including statement logging and duration tracking
- The fast-path interface cannot handle collation-sensitive functions (uses InvalidOid for collation)
- Caching of function lookup information was removed in favor of fresh lookups on every call