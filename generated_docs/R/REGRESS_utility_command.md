# REGRESS_utility_command

## Location
[src/test/modules/test_oat_hooks/test_oat_hooks.c:380-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_oat_hooks/test_oat_hooks.c#L380-L418)

## Overview
A test hook function that intercepts utility command processing for regression testing, providing auditing capabilities and enforcing restrictions on utility command execution.

## Definition
```c
static void REGRESS_utility_command(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree, ProcessUtilityContext context, ParamListInfo params, QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc)
```

## Detailed Description
This function serves as a custom ProcessUtility hook specifically designed for PostgreSQL regression testing. It implements the ProcessUtility_hook interface to monitor and control the execution of utility commands (DDL commands like CREATE, ALTER, DROP, etc.). The function performs several key operations:

1. **Command Identification**: Extracts the utility statement from the planned statement and determines the command type using GetCommandTagName()
2. **Auditing**: Records both attempted and successful utility command executions using audit functions
3. **Permission Enforcement**: Can deny all utility commands to non-superusers when the REGRESS_deny_utility_commands flag is enabled
4. **Hook Chaining**: Forwards the call to the next hook in the chain, or calls standard_ProcessUtility() if no next hook exists

The function ensures proper processing flow by either calling the next hook in the chain or falling back to the standard utility processing function.

## Parameters / Member Variables
- `pstmt`: Planned statement containing the utility statement to be executed
- `queryString`: Original query string that generated this utility command
- `readOnlyTree`: Boolean indicating whether the parse tree should be treated as read-only
- `context`: Context in which the utility command is being processed
- `params`: Parameter list information for parameterized queries
- `queryEnv`: Query environment containing additional context information
- `dest`: Destination receiver for query results
- `qc`: Query completion structure for tracking command execution status

## Dependencies
- Functions called/Symbols referenced:
  - [GetCommandTagName](../G/GetCommandTagName.md)
  - [CreateCommandTag](../C/CreateCommandTag.md)
  - [audit_attempt](../a/audit_attempt.md)
  - superuser_arg
  - [GetUserId](../G/GetUserId.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)
  - [audit_success](../a/audit_success.md)
  - [pstrdup](../p/pstrdup.md)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (hook installation)

## Notes and Other Information
- This is a static function used exclusively for regression testing in the test_oat_hooks module
- The function implements a simple allow/deny mechanism for all utility commands via the REGRESS_deny_utility_commands global flag
- Superusers bypass the denial mechanism and can always execute utility commands
- Maintains proper hook chaining by calling next_ProcessUtility_hook if available, otherwise falls back to standard_ProcessUtility()
- Uses PostgreSQL's command tag system to identify and log the specific type of utility command being executed
- Part of the utility command hook testing framework for validating PostgreSQL's DDL execution control
- The hook intercepts all utility commands including CREATE, ALTER, DROP, GRANT, REVOKE, and other DDL statements
- Provides comprehensive auditing by logging both attempts and successful completions of utility commands
- Used to test scenarios where non-superusers are denied the ability to execute DDL commands during regression testing