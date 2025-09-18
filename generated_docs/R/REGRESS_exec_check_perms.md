# REGRESS_exec_check_perms

## Location
[src/test/modules/test_oat_hooks/test_oat_hooks.c:348-379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_oat_hooks/test_oat_hooks.c#L348-L379)

## Overview
A test hook function that intercepts executor permission checks for regression testing, providing auditing capabilities and enforcing execution restrictions on SQL statements.

## Definition
```c
static bool REGRESS_exec_check_perms(List *rangeTabls, List *rteperminfos, bool do_abort)
```

## Detailed Description
This function serves as a custom executor permission check hook specifically designed for PostgreSQL regression testing. It implements the ExecutorCheckPerms_hook interface to monitor and control the execution of SQL statements during the executor phase. The function performs several key operations:

1. **Permission Evaluation**: Determines whether execution should be allowed based on the REGRESS_deny_exec_perms flag and superuser status
2. **Auditing**: Records attempted, successful, and failed executor permission checks using audit functions
3. **Hook Chaining**: Forwards the call to the next hook in the chain and respects its decision
4. **Error Handling**: Can abort execution with an error message when do_abort is true and permission is denied

The function returns a boolean indicating whether execution should be allowed, and optionally aborts the operation if configured to do so.

## Parameters / Member Variables
- `rangeTabls`: List of range table entries representing tables/relations involved in the query
- `rteperminfos`: List of range table permission info structures containing permission details
- `do_abort`: Boolean flag indicating whether to abort execution with an error if permission is denied

## Dependencies
- Functions called/Symbols referenced:
  - superuser_arg
  - [GetUserId](../G/GetUserId.md)
  - [audit_attempt](../a/audit_attempt.md)
  - [audit_success](../a/audit_success.md)
  - [audit_failure](../a/audit_failure.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [pstrdup](../p/pstrdup.md)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (hook installation)

## Notes and Other Information
- This is a static function used exclusively for regression testing in the test_oat_hooks module
- The function implements a simple allow/deny mechanism based on the REGRESS_deny_exec_perms global flag
- Superusers bypass the denial mechanism and are always allowed to execute
- The hook maintains proper chaining by calling next_exec_check_perms_hook if it exists
- Returns false if either the current hook or any subsequent hook in the chain denies permission
- Provides comprehensive auditing by logging attempts, successes, and failures separately
- Part of the executor hook testing framework for validating PostgreSQL's execution permission system
- The do_abort parameter allows the hook to either silently deny permission or actively abort the operation
- Used to test scenarios where non-superusers are denied execution permissions during regression testing