COMMIT  
---  
[Prev](sql-comment.md "COMMENT") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-commit-prepared.md "COMMIT PREPARED")  
  
* * *

## COMMIT

COMMIT — commit the current transaction

## Synopsis
    
    
    COMMIT [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ]
    

## Description

`COMMIT` commits the current transaction. All changes made by the transaction become visible to others and are guaranteed to be durable if a crash occurs. 

## Parameters

`WORK`  
`TRANSACTION` #
    

Optional key words. They have no effect. 

`AND CHAIN` #
    

If `AND CHAIN` is specified, a new transaction is immediately started with the same transaction characteristics (see [SET TRANSACTION](sql-set-transaction.md "SET TRANSACTION")) as the just finished one. Otherwise, no new transaction is started. 

## Notes

Use [ROLLBACK](sql-rollback.md "ROLLBACK") to abort a transaction. 

Issuing `COMMIT` when not inside a transaction does no harm, but it will provoke a warning message. `COMMIT AND CHAIN` when not inside a transaction is an error. 

## Examples

To commit the current transaction and make all changes permanent: 
    
    
    COMMIT;
    

## Compatibility

The command `COMMIT` conforms to the SQL standard. The form `COMMIT TRANSACTION` is a PostgreSQL extension. 

## See Also

[BEGIN](sql-begin.md "BEGIN"), [ROLLBACK](sql-rollback.md "ROLLBACK")

* * *

[Prev](sql-comment.md "COMMENT") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-commit-prepared.md "COMMIT PREPARED")  
---|---|---  
COMMENT | [Home](index.md "PostgreSQL 17.5 Documentation")|  COMMIT PREPARED
