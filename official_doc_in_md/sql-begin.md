BEGIN  
---  
[Prev](sql-analyze.md "ANALYZE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-call.md "CALL")  
  
* * *

## BEGIN

BEGIN — start a transaction block

## Synopsis
    
    
    BEGIN [ WORK | TRANSACTION ] [ _transaction_mode_ [, ...] ]
    
    where _transaction_mode_ is one of:
    
        ISOLATION LEVEL { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }
        READ WRITE | READ ONLY
        [ NOT ] DEFERRABLE
    

## Description

`BEGIN` initiates a transaction block, that is, all statements after a `BEGIN` command will be executed in a single transaction until an explicit [`COMMIT`](sql-commit.md "COMMIT") or [`ROLLBACK`](sql-rollback.md "ROLLBACK") is given. By default (without `BEGIN`), PostgreSQL executes transactions in “autocommit” mode, that is, each statement is executed in its own transaction and a commit is implicitly performed at the end of the statement (if execution was successful, otherwise a rollback is done). 

Statements are executed more quickly in a transaction block, because transaction start/commit requires significant CPU and disk activity. Execution of multiple statements inside a transaction is also useful to ensure consistency when making several related changes: other sessions will be unable to see the intermediate states wherein not all the related updates have been done. 

If the isolation level, read/write mode, or deferrable mode is specified, the new transaction has those characteristics, as if [`SET TRANSACTION`](sql-set-transaction.md "SET TRANSACTION") was executed. 

## Parameters

`WORK`  
`TRANSACTION`
    

Optional key words. They have no effect. 

Refer to [SET TRANSACTION](sql-set-transaction.md "SET TRANSACTION") for information on the meaning of the other parameters to this statement. 

## Notes

[`START TRANSACTION`](sql-start-transaction.md "START TRANSACTION") has the same functionality as `BEGIN`. 

Use [`COMMIT`](sql-commit.md "COMMIT") or [`ROLLBACK`](sql-rollback.md "ROLLBACK") to terminate a transaction block. 

Issuing `BEGIN` when already inside a transaction block will provoke a warning message. The state of the transaction is not affected. To nest transactions within a transaction block, use savepoints (see [SAVEPOINT](sql-savepoint.md "SAVEPOINT")). 

For reasons of backwards compatibility, the commas between successive _`transaction_modes`_ can be omitted. 

## Examples

To begin a transaction block: 
    
    
    BEGIN;
    

## Compatibility

`BEGIN` is a PostgreSQL language extension. It is equivalent to the SQL-standard command [`START TRANSACTION`](sql-start-transaction.md "START TRANSACTION"), whose reference page contains additional compatibility information. 

The `DEFERRABLE` _`transaction_mode`_ is a PostgreSQL language extension. 

Incidentally, the `BEGIN` key word is used for a different purpose in embedded SQL. You are advised to be careful about the transaction semantics when porting database applications. 

## See Also

[COMMIT](sql-commit.md "COMMIT"), [ROLLBACK](sql-rollback.md "ROLLBACK"), [START TRANSACTION](sql-start-transaction.md "START TRANSACTION"), [SAVEPOINT](sql-savepoint.md "SAVEPOINT")

* * *

[Prev](sql-analyze.md "ANALYZE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-call.md "CALL")  
---|---|---  
ANALYZE | [Home](index.md "PostgreSQL 17.5 Documentation")|  CALL
