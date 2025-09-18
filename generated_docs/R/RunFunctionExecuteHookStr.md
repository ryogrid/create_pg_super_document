# RunFunctionExecuteHookStr

## Location
[src/backend/catalog/objectaccess.c:265-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaccess.c#L265-L273)

## Overview
Invokes the string-based object access hook for function execution events, allowing extensions to monitor or control function executions by function name rather than OID.

## Definition
```c
void RunFunctionExecuteHookStr(const char *objectName)
```

## Detailed Description
This function serves as the entrypoint for the OAT_FUNCTION_EXECUTE object access hook when working with function names as strings instead of OIDs. It provides a mechanism for PostgreSQL extensions to intercept function execution events based on the function name. The function directly invokes the registered string-based object access hook (object_access_hook_str) with the appropriate parameters to notify extensions when a function is about to be executed.

This hook is part of PostgreSQL's extensible object access control system and is typically used for auditing, logging, security enforcement, or other monitoring purposes. Unlike some other object access hooks that can deny operations, this hook is primarily informational - it notifies extensions about function execution events but doesn't provide a mechanism to prevent the execution.

The function follows the standard PostgreSQL object access hook pattern where extensions register callback functions that are invoked at specific points during database operations. This particular hook is called during function execution to allow extensions to perform custom logic based on which functions are being executed.

## Parameters / Member Variables
- `objectName`: The name of the function being executed as a C string

## Dependencies
- Functions called/Symbols referenced:
  - OAT_FUNCTION_EXECUTE (object access type constant for function execution events)
  - object_access_hook_str (global hook function pointer for string-based hooks)
  - ProcedureRelationId (system catalog relation ID constant for pg_proc)
- Called from (representative examples):
  - InvokeFunctionExecuteHookStr (wrapper macro/function)

## Notes and Other Information
- Requires that object_access_hook_str is not NULL (checked by assertion)
- Returns void - this is a notification hook, not an access control hook
- No additional hook-specific arguments are passed (NULL is passed as the arg parameter)
- Part of PostgreSQL's extensible object access control system that allows third-party extensions to monitor database operations
- This is the string-based variant; there's also RunFunctionExecuteHook() that works with OIDs
- Commonly used for security auditing, query logging, or implementing custom monitoring solutions
- The hook is called with ProcedureRelationId to indicate that the object being referenced is from the pg_proc system catalog