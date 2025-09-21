END  
---  
[Prev](sql-dropview.md "DROP VIEW") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-execute.md "EXECUTE")  
  
* * *

## END

END — commit the current transaction

## Synopsis
    
    
    END [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ]
    

## Description

`END` commits the current transaction. All changes made by the transaction become visible to others and are guaranteed to be durable if a crash occurs. This command is a PostgreSQL extension that is equivalent to [`COMMIT`](sql-commit.md "COMMIT"). 

## Parameters

`WORK`  
`TRANSACTION`
    

Optional key words. They have no effect. 

`AND CHAIN`
    

If `AND CHAIN` is specified, a new transaction is immediately started with the same transaction characteristics (see [SET TRANSACTION](sql-set-transaction.md "SET TRANSACTION")) as the just finished one. Otherwise, no new transaction is started. 

## Notes

Use [`ROLLBACK`](sql-rollback.md "ROLLBACK") to abort a transaction. 

Issuing `END` when not inside a transaction does no harm, but it will provoke a warning message. 

## Examples

To commit the current transaction and make all changes permanent: 
    
    
    END;
    

## Compatibility

`END` is a PostgreSQL extension that provides functionality equivalent to [`COMMIT`](sql-commit.md "COMMIT"), which is specified in the SQL standard. 

## See Also

[BEGIN](sql-begin.md "BEGIN"), [COMMIT](sql-commit.md "COMMIT"), [ROLLBACK](sql-rollback.md "ROLLBACK")

* * *

[Prev](sql-dropview.md "DROP VIEW") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-execute.md "EXECUTE")  
---|---|---  
DROP VIEW | [Home](index.md "PostgreSQL 17.5 Documentation")|  EXECUTE
