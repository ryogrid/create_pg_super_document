# standard_ProcessUtility

## Location
[src/backend/tcop/utility.c:540-1088](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L540-L1088)

## Overview
standard_ProcessUtility is the core implementation function that handles execution of utility commands that do not require event trigger support, implementing PostgreSQL's default utility command processing logic.

## Definition


## Detailed Description
standard_ProcessUtility serves as PostgreSQL's primary utility command execution engine, handling a comprehensive range of SQL commands including transaction control, DDL operations, administrative commands, and system maintenance operations. The function operates through a sophisticated dispatching mechanism using a large switch statement based on the node type of the parsed statement.

The function is strategically designed to handle only commands that do not require event trigger support, while delegating event-trigger-enabled commands to ProcessUtilitySlow. This architectural separation is critical for performance and correctness - certain commands like START TRANSACTION must avoid event trigger processing because they might need to refresh the event trigger cache, which requires being in a valid transaction state.

Key operational aspects include: stack depth checking for recursion protection, optional statement tree copying for read-only scenarios, comprehensive read-only and parallel mode validation, transaction context management, and systematic command routing to specialized execution functions.

## Parameters / Member Variables
- `pstmt`: PlannedStmt wrapper containing the utility statement and execution metadata
- `queryString`: Original source text of the command for error reporting and logging
- `readOnlyTree`: Boolean flag indicating whether the statement tree should be copied to prevent modifications
- `context`: ProcessUtilityContext specifying execution context (toplevel, non-toplevel, or subcommand)
- `params`: ParamListInfo providing parameter values for parameterized statements
- `queryEnv`: QueryEnvironment containing parse-through-execution environment including ephemeral tables
- `dest`: DestReceiver specifying the destination for command results
- `qc`: QueryCompletion structure for tracking command completion status and affected row counts

## Dependencies
- Functions called/Symbols referenced:
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (for commands requiring event trigger support)
  - [CheckRestrictedOperation](../C/CheckRestrictedOperation.md) (security validation for sensitive commands)
  - Various command-specific execution functions (BeginTransactionBlock, CreateTableSpace, ExecuteTruncate, etc.)
  - Transaction control functions (PreventInTransactionBlock, RequireTransactionBlock)
  - Security and privilege checking functions (has_privs_of_role, superuser)
  - Event trigger support checking (EventTriggerSupportsObjectType)
- Called from (representative examples):
  - [ProcessUtility](../P/ProcessUtility.md) (main entry point when no hooks are present)
  - [REGRESS_utility_command](../R/REGRESS_utility_command.md) (testing framework)

## Notes and Other Information
- The function handles approximately 40+ different statement types through its central switch statement
- Critical architectural decision: event trigger avoidance for transaction control commands prevents circular dependencies
- Implements sophisticated read-only mode validation using ClassifyUtilityCommandAsReadOnly to prevent write operations in inappropriate contexts
- Global object commands (databases, roles, tablespaces) explicitly bypass event triggers since they operate at cluster level
- Security-sensitive commands like LISTEN/UNLISTEN include additional validation (background process restrictions, security context checks)
- The function includes performance optimizations by fast-pathing commands that conditionally support event triggers
- Uses CommandCounterIncrement at completion to make command effects visible for subsequent operations within the same transaction
- Supports recursive processing for complex commands like CREATE SCHEMA that contain embedded utility statements