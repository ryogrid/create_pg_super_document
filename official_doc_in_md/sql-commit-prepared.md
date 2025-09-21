COMMIT PREPARED  
---  
[Prev](sql-commit.md "COMMIT") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-copy.md "COPY")  
  
* * *

## COMMIT PREPARED

COMMIT PREPARED — commit a transaction that was earlier prepared for two-phase commit

## Synopsis
    
    
    COMMIT PREPARED _transaction_id_
    

## Description

`COMMIT PREPARED` commits a transaction that is in prepared state. 

## Parameters

 _`transaction_id`_
    

The transaction identifier of the transaction that is to be committed. 

## Notes

To commit a prepared transaction, you must be either the same user that executed the transaction originally, or a superuser. But you do not have to be in the same session that executed the transaction. 

This command cannot be executed inside a transaction block. The prepared transaction is committed immediately. 

All currently available prepared transactions are listed in the [`pg_prepared_xacts`](view-pg-prepared-xacts.md "52.16. pg_prepared_xacts") system view. 

## Examples

Commit the transaction identified by the transaction identifier `foobar`: 
    
    
    COMMIT PREPARED 'foobar';
    

## Compatibility

`COMMIT PREPARED` is a PostgreSQL extension. It is intended for use by external transaction management systems, some of which are covered by standards (such as X/Open XA), but the SQL side of those systems is not standardized. 

## See Also

[PREPARE TRANSACTION](sql-prepare-transaction.md "PREPARE TRANSACTION"), [ROLLBACK PREPARED](sql-rollback-prepared.md "ROLLBACK PREPARED")

* * *

[Prev](sql-commit.md "COMMIT") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-copy.md "COPY")  
---|---|---  
COMMIT | [Home](index.md "PostgreSQL 17.5 Documentation")|  COPY
