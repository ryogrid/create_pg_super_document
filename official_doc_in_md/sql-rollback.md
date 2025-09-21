ROLLBACK  
---  
[Prev](sql-revoke.md "REVOKE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-rollback-prepared.md "ROLLBACK PREPARED")  
  
* * *

## ROLLBACK

ROLLBACK — abort the current transaction

## Synopsis
    
    
    ROLLBACK [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ]
    

## Description

`ROLLBACK` rolls back the current transaction and causes all the updates made by the transaction to be discarded. 

## Parameters

`WORK`  
`TRANSACTION` #
    

Optional key words. They have no effect. 

`AND CHAIN` #
    

If `AND CHAIN` is specified, a new (not aborted) transaction is immediately started with the same transaction characteristics (see [SET TRANSACTION](sql-set-transaction.md "SET TRANSACTION")) as the just finished one. Otherwise, no new transaction is started. 

## Notes

Use [`COMMIT`](sql-commit.md "COMMIT") to successfully terminate a transaction. 

Issuing `ROLLBACK` outside of a transaction block emits a warning and otherwise has no effect. `ROLLBACK AND CHAIN` outside of a transaction block is an error. 

## Examples

To abort all changes: 
    
    
    ROLLBACK;
    

## Compatibility

The command `ROLLBACK` conforms to the SQL standard. The form `ROLLBACK TRANSACTION` is a PostgreSQL extension. 

## See Also

[BEGIN](sql-begin.md "BEGIN"), [COMMIT](sql-commit.md "COMMIT"), [ROLLBACK TO SAVEPOINT](sql-rollback-to.md "ROLLBACK TO SAVEPOINT")

* * *

[Prev](sql-revoke.md "REVOKE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-rollback-prepared.md "ROLLBACK PREPARED")  
---|---|---  
REVOKE | [Home](index.md "PostgreSQL 17.5 Documentation")|  ROLLBACK PREPARED
