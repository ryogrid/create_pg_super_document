ROLLBACK PREPARED  
---  
[Prev](sql-rollback.md "ROLLBACK") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-rollback-to.md "ROLLBACK TO SAVEPOINT")  
  
* * *

## ROLLBACK PREPARED

ROLLBACK PREPARED — cancel a transaction that was earlier prepared for two-phase commit

## Synopsis
    
    
    ROLLBACK PREPARED _transaction_id_
    

## Description

`ROLLBACK PREPARED` rolls back a transaction that is in prepared state. 

## Parameters

 _`transaction_id`_
    

The transaction identifier of the transaction that is to be rolled back. 

## Notes

To roll back a prepared transaction, you must be either the same user that executed the transaction originally, or a superuser. But you do not have to be in the same session that executed the transaction. 

This command cannot be executed inside a transaction block. The prepared transaction is rolled back immediately. 

All currently available prepared transactions are listed in the [`pg_prepared_xacts`](view-pg-prepared-xacts.md "52.16. pg_prepared_xacts") system view. 

## Examples

Roll back the transaction identified by the transaction identifier `foobar`: 
    
    
    ROLLBACK PREPARED 'foobar';
    

## Compatibility

`ROLLBACK PREPARED` is a PostgreSQL extension. It is intended for use by external transaction management systems, some of which are covered by standards (such as X/Open XA), but the SQL side of those systems is not standardized. 

## See Also

[PREPARE TRANSACTION](sql-prepare-transaction.md "PREPARE TRANSACTION"), [COMMIT PREPARED](sql-commit-prepared.md "COMMIT PREPARED")

* * *

[Prev](sql-rollback.md "ROLLBACK") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-rollback-to.md "ROLLBACK TO SAVEPOINT")  
---|---|---  
ROLLBACK | [Home](index.md "PostgreSQL 17.5 Documentation")|  ROLLBACK TO SAVEPOINT
