# emit_audit_message

## Location
src/test/modules/test_oat_hooks/test_oat_hooks.c: 231 - 258

## Overview
A static utility function in the test_oat_hooks module that emits audit messages for testing object access control hooks, providing deterministic logging for regression tests.

## Definition
```c
static void emit_audit_message(const char *type, const char *hook, char *action, char *objName)
```

## Detailed Description
emit_audit_message is a core utility function in PostgreSQL's test_oat_hooks testing module that generates standardized audit messages for object access control testing. The function ensures test result determinism by only emitting messages from leader processes (not parallel workers) when regression auditing is enabled. It formats audit messages with contextual information including the operation type, hook name, action being performed, and optionally the object name. The function also handles memory cleanup for dynamically allocated action and objName parameters.

The audit messages follow a structured format that includes user privilege information (superuser vs non-superuser) to help verify that access control mechanisms are working correctly across different privilege levels.

## Parameters / Member Variables
- `type`: A string describing the operation type (e.g., "attempting", "finished", "denied")
- `hook`: The name of the hook being tested (e.g., "object_access_hook_str")
- `action`: A dynamically allocated string describing the specific action (freed by this function)
- `objName`: A dynamically allocated string containing the object name (freed by this function, may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - REGRESS_audit (global variable controlling audit output)
  - IsParallelWorker() (checks if running in parallel worker process)
  - superuser_arg() (checks superuser status)
  - [GetUserId](../G/GetUserId.md)() (gets current user ID)
  - ereport() (PostgreSQL error/notice reporting)
  - [pfree](../p/pfree.md)() (PostgreSQL memory deallocation)
- Called from (representative examples):
  - [audit_attempt](../a/audit_attempt.md)() (for logging access attempts)
  - [audit_success](../a/audit_success.md)() (for logging successful operations)
  - [audit_failure](../a/audit_failure.md)() (for logging denied operations)

## Notes and Other Information
- This function is part of the object access testing framework for mandatory access control (MAC)
- Messages are only emitted when REGRESS_audit is enabled to avoid test noise
- Parallel worker filtering ensures deterministic test results when debug_parallel_query = regress
- Located at src/test/modules/test_oat_hooks/test_oat_hooks.c:231-258
- Takes ownership of and frees the action and objName parameters
- Uses ERRCODE_INTERNAL_ERROR for all audit messages as they are informational notices