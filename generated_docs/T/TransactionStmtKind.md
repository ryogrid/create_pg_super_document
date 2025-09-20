# TransactionStmtKind

## Location
[src/include/nodes/parsenodes.h:3652-3664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3652-L3664)

## Overview
TransactionStmtKind is an enumeration type that defines the different kinds of transaction-related SQL statements in PostgreSQL, including transaction control, savepoint operations, and prepared transaction commands.

## Definition

```c
typedef enum TransactionStmtKind
{
	TRANS_STMT_BEGIN,
	TRANS_STMT_START,			/* semantically identical to BEGIN */
	TRANS_STMT_COMMIT,
	TRANS_STMT_ROLLBACK,
	TRANS_STMT_SAVEPOINT,
	TRANS_STMT_RELEASE,
	TRANS_STMT_ROLLBACK_TO,
	TRANS_STMT_PREPARE,
	TRANS_STMT_COMMIT_PREPARED,
	TRANS_STMT_ROLLBACK_PREPARED,
} TransactionStmtKind;
```
## Detailed Description
TransactionStmtKind categorizes the various transaction control statements supported by PostgreSQL. It covers three main areas of transaction management:

1. **Basic Transaction Control**: BEGIN/START, COMMIT, and ROLLBACK operations that manage the fundamental transaction lifecycle.

2. **Savepoint Operations**: SAVEPOINT, RELEASE, and ROLLBACK TO commands that provide nested transaction-like behavior within a single transaction.

3. **Prepared Transactions**: PREPARE, COMMIT PREPARED, and ROLLBACK PREPARED statements for two-phase commit protocols used in distributed transactions.

This enumeration is used primarily in the TransactionStmt parse node structure to distinguish between different transaction statement types during parsing and execution.

## Parameters / Member Variables
- `TRANS_STMT_BEGIN`: Begin a new transaction block
- `TRANS_STMT_START`: Start a new transaction block (identical to BEGIN)
- `TRANS_STMT_COMMIT`: Commit the current transaction
- `TRANS_STMT_ROLLBACK`: Rollback the current transaction
- `TRANS_STMT_SAVEPOINT`: Create a savepoint within the transaction
- `TRANS_STMT_RELEASE`: Release a savepoint (commit its effects)
- `TRANS_STMT_ROLLBACK_TO`: Rollback to a specific savepoint
- `TRANS_STMT_PREPARE`: Prepare a transaction for two-phase commit
- `TRANS_STMT_COMMIT_PREPARED`: Commit a previously prepared transaction
- `TRANS_STMT_ROLLBACK_PREPARED`: Rollback a previously prepared transaction

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - TransactionStmt (src/include/nodes/parsenodes.h:3670)

## Notes and Other Information
- TRANS_STMT_START and TRANS_STMT_BEGIN are functionally identical, both representing the SQL BEGIN statement
- Savepoint operations (SAVEPOINT, RELEASE, ROLLBACK_TO) provide nested transaction semantics within a single transaction
- Prepared transaction operations support distributed transaction protocols and two-phase commit
- This enumeration is part of PostgreSQL's SQL parser infrastructure and is used to distinguish between different transaction statement types during statement processing
- The prepared transaction features are primarily used in distributed database scenarios and require specific configuration to be enabled