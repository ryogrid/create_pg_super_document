# TransactionStmt

## Location
[src/include/nodes/parsenodes.h:3667-3679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3667-L3679)

## Overview
TransactionStmt represents a transaction control statement (BEGIN, COMMIT, ROLLBACK, SAVEPOINT, etc.) in PostgreSQL's parse tree structure.

## Definition
```c
typedef struct TransactionStmt
{
    NodeTag         type;
    TransactionStmtKind kind;         /* see above */
    List           *options;          /* for BEGIN/START commands */
    /* for savepoint commands */
    char           *savepoint_name pg_node_attr(query_jumble_ignore);
    /* for two-phase-commit related commands */
    char           *gid pg_node_attr(query_jumble_ignore);
    bool            chain;            /* AND CHAIN option */
    /* token location, or -1 if unknown */
    ParseLoc        location pg_node_attr(query_jumble_location);
} TransactionStmt;
```

## Detailed Description
TransactionStmt is a parse tree node that encapsulates all forms of transaction control statements in PostgreSQL. It supports basic transaction operations (BEGIN, COMMIT, ROLLBACK), savepoint management, and two-phase commit operations. The structure is designed to handle the various options and parameters that can accompany different transaction statements, making it a unified representation for all transaction-related SQL commands.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a TransactionStmt node
- `kind`: TransactionStmtKind enum specifying the type of transaction statement (BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, ROLLBACK_TO, PREPARE, COMMIT_PREPARED, ROLLBACK_PREPARED)
- `options`: List of options for BEGIN/START commands (e.g., isolation level, read-only mode)
- `savepoint_name`: Name of the savepoint for savepoint-related commands (SAVEPOINT, RELEASE, ROLLBACK TO)
- `gid`: Global transaction identifier for two-phase commit operations (PREPARE, COMMIT PREPARED, ROLLBACK PREPARED)
- `chain`: Boolean flag indicating whether the AND CHAIN option was specified
- `location`: Parse location of the statement token for error reporting and debugging

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionStmtKind](TransactionStmtKind.md)
  - ParseLoc
  - NodeTag
  - [List](../L/List.md)
- Called from (representative examples):
  - [execute_sql_string](../e/execute_sql_string.md)
  - [exec_simple_query](../e/exec_simple_query.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)
  - [IsTransactionExitStmt](../I/IsTransactionExitStmt.md)
  - [ClassifyUtilityCommandAsReadOnly](../C/ClassifyUtilityCommandAsReadOnly.md)

## Notes and Other Information
- The pg_node_attr annotations indicate special handling during query jumbling for plan caching and fingerprinting
- [TransactionStmt](TransactionStmt.md) supports all PostgreSQL transaction control features including nested transactions via savepoints and distributed transactions via two-phase commit
- The chain field supports the SQL standard COMMIT AND CHAIN and ROLLBACK AND CHAIN syntax
- Location tracking enables precise error reporting for syntax errors in transaction statements