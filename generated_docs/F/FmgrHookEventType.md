# FmgrHookEventType

## Location
[src/include/fmgr.h:787-796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fmgr.h#L787-L796)

## Overview
An enumeration that defines event types for function manager hooks, used by security policy modules to track function execution lifecycle events.

## Definition

```c
typedef bool (*needs_fmgr_hook_type) (Oid fn_oid);
```
## Detailed Description
The `FmgrHookEventType` enumeration defines the different events that can occur during function execution in PostgreSQL's function manager (fmgr). This enum is part of the hook system that allows plugin modules, particularly security policy modules like SELinux integration (sepgsql), to monitor and potentially intervene in function calls.

The hook system enables loadable security policy modules to perform additional privilege checks, maintain security contexts, or perform other internal bookkeeping operations during function execution. The enum values correspond to different phases of function execution, allowing hooks to respond appropriately to normal execution flow as well as error conditions.

Hook functions receive these event types along with function information and can take appropriate actions based on the current execution phase. This is particularly important for security modules that need to maintain consistent security contexts across function boundaries and handle cleanup during error conditions.

## Parameters / Member Variables
- `FHET_START`: Indicates the beginning of function execution, triggered before the actual function call
- `FHET_END`: Indicates successful completion of function execution, triggered after the function returns normally
- `FHET_ABORT`: Indicates function execution was terminated due to an error or exception, used for cleanup operations

## Dependencies
- Functions called/Symbols referenced:
  - Used in `fmgr_hook_type` function pointer typedef
- Called from (representative examples):
  - Function execution in src/backend/utils/fmgr/fmgr.c (via `fmgr_hook` calls)
  - `sepgsql_fmgr_hook` in contrib/sepgsql/label.c:311 (security policy implementation)

## Notes and Other Information
- The enum is defined in src/include/fmgr.h:782-787
- Part of the pluggable security architecture that allows external modules to hook into function execution
- Primarily used by security policy modules like SELinux integration (sepgsql contrib module)
- The hook system must handle all three event types to maintain consistency, especially for security context management
- The FHET_ABORT event is crucial for proper cleanup when functions terminate abnormally due to exceptions or errors
- Hook functions are called through the global `fmgr_hook` function pointer if registered