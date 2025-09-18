# ProcessUtility

## Location
[src/backend/tcop/utility.c:499-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L499-L539)

## Overview
ProcessUtility is the main entry point and dispatcher for executing all utility commands in PostgreSQL, providing a hook mechanism for extensions and delegating actual execution to standard_ProcessUtility.

## Definition


## Detailed Description
ProcessUtility serves as the central gateway for all utility command execution in PostgreSQL. It acts as both a validation layer and a plugin extensibility point. The function first performs essential parameter validation, ensuring that the incoming PlannedStmt is properly formed for a utility command and that required parameters are present.

The core functionality revolves around the ProcessUtility_hook mechanism, which allows loadable plugins and extensions to intercept and potentially modify utility command processing. If a hook is registered, it takes control of the execution; otherwise, the function delegates to standard_ProcessUtility for default processing.

This design pattern enables sophisticated extensions like logical replication, audit logging, or security plugins to integrate seamlessly into PostgreSQL's command processing pipeline without requiring core code modifications.

## Parameters / Member Variables
- `pstmt`: PlannedStmt wrapper containing the utility statement and metadata including statement location and length
- `queryString`: The original source text of the command (must not be NULL; constant strings acceptable if source unavailable)
- `readOnlyTree`: Boolean flag indicating whether the statement's node tree must remain unmodified during processing
- `context`: ProcessUtilityContext enum identifying the source context (toplevel client command, non-toplevel client command, or subcommand of larger utility)
- `params`: ParamListInfo containing parameters to use during statement execution (may be NULL)
- `queryEnv`: QueryEnvironment providing parse-through-execution environment including ephemeral named tables like trigger transition tables (may be NULL)
- `dest`: DestReceiver specifying where to send command results
- `qc`: QueryCompletion structure for storing command completion status data (may be NULL, but if provided must be pre-initialized)

## Dependencies
- Functions called/Symbols referenced:
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (default implementation when no hook is present)
  - ProcessUtility_hook (function pointer for plugin extension points)
  - IsA (macro for type checking PlannedStmt)
- Called from (representative examples):
  - [PortalRunUtility](PortalRunUtility.md) (portal execution system)
  - [execute_sql_string](../e/execute_sql_string.md) (extension loading)
  - [CreateSchemaCommand](../C/CreateSchemaCommand.md) (schema creation with embedded statements)
  - [ProcessUtilitySlow](ProcessUtilitySlow.md) (recursive processing)
  - [ProcessUtilityForAlterTable](ProcessUtilityForAlterTable.md) (ALTER TABLE subcommand processing)

## Notes and Other Information
- The function includes comprehensive parameter validation with multiple Assert statements to catch programming errors early
- The queryString parameter became mandatory as of PostgreSQL 8.4 (NULL no longer accepted)
- Multiple invocations may receive the same queryString when processing semicolon-separated statements; use pstmt->stmt_location and pstmt->stmt_len to identify individual statement boundaries
- Some utility statements like CREATE SCHEMA recursively call ProcessUtility for sub-statements, often passing the same queryString and location information
- The hook mechanism is designed for extensions that typically call standard_ProcessUtility after performing their own processing
- [Command](../C/Command.md) completion tracking through QueryCompletion parameter supports proper statistics and monitoring