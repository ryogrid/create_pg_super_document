# GetCommandLogLevel

## Location
[src/backend/tcop/utility.c:3247-3768](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L3247-L3768)

## Overview
GetCommandLogLevel is a utility function that determines the minimum log_statement level required for a PostgreSQL command to be logged, supporting raw parse trees, analyzed Queries, and PlannedStmts.

## Definition
```c
LogStmtLevel GetCommandLogLevel(Node *parsetree)
```

## Detailed Description
This function implements PostgreSQL's statement logging classification system by analyzing SQL commands and assigning them to appropriate logging levels. It serves as the core logic for the log_statement configuration parameter, which controls which types of statements are logged based on their operational impact.

The function categorizes commands into three main logging levels:
- LOGSTMT_ALL: Statements that are safe and informational (SELECT, transaction control, etc.)
- LOGSTMT_MOD: Data modification statements (INSERT, UPDATE, DELETE, MERGE, TRUNCATE)
- LOGSTMT_DDL: Data definition statements that modify database structure (CREATE, ALTER, DROP, etc.)

The function recursively processes complex statement types (PREPARE, EXECUTE, EXPLAIN ANALYZE) to determine the logging level of the underlying command. It handles both raw and processed statement forms uniformly for utility commands.

## Parameters / Member Variables
- `parsetree`: A Node pointer representing the command to classify, which can be a raw parse tree, analyzed Query, or PlannedStmt

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine the node type)
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md) (for EXECUTE statements)
  - [defGetBoolean](../d/defGetBoolean.md) (for EXPLAIN option processing)
  - [LogStmtLevel](../L/LogStmtLevel.md) constants (LOGSTMT_ALL, LOGSTMT_MOD, LOGSTMT_DDL)
  - Various statement structures (SelectStmt, CopyStmt, etc.)
- Called from (representative examples):
  - [check_log_statement](../c/check_log_statement.md) (src/backend/tcop/postgres.c:2382)
  - [CreateCommandName](../C/CreateCommandName.md) (src/include/tcop/utility.h:108)

## Notes and Other Information
- The function is recursive for RawStmt, PREPARE, EXECUTE, and EXPLAIN ANALYZE statements
- COPY statements are classified as LOGSTMT_MOD when importing (is_from=true) and LOGSTMT_ALL when exporting
- SELECT INTO is classified as LOGSTMT_DDL due to its table creation behavior
- EXPLAIN ANALYZE recursively analyzes the contained statement, while plain EXPLAIN is LOGSTMT_ALL
- For unrecognized node types or command types, it defaults to LOGSTMT_ALL for safety
- The function supports PostgreSQL's hierarchical logging configuration where higher levels include lower levels
- Critical for database auditing and compliance requirements where different statement types require different logging policies