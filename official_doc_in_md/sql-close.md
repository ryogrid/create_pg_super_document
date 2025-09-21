CLOSE  
---  
[Prev](sql-checkpoint.md "CHECKPOINT") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-cluster.md "CLUSTER")  
  
* * *

## CLOSE

CLOSE — close a cursor

## Synopsis
    
    
    CLOSE { _name_ | ALL }
    

## Description

`CLOSE` frees the resources associated with an open cursor. After the cursor is closed, no subsequent operations are allowed on it. A cursor should be closed when it is no longer needed. 

Every non-holdable open cursor is implicitly closed when a transaction is terminated by `COMMIT` or `ROLLBACK`. A holdable cursor is implicitly closed if the transaction that created it aborts via `ROLLBACK`. If the creating transaction successfully commits, the holdable cursor remains open until an explicit `CLOSE` is executed, or the client disconnects. 

## Parameters

 _`name`_
    

The name of an open cursor to close. 

`ALL`
    

Close all open cursors. 

## Notes

PostgreSQL does not have an explicit `OPEN` cursor statement; a cursor is considered open when it is declared. Use the [`DECLARE`](sql-declare.md "DECLARE") statement to declare a cursor. 

You can see all available cursors by querying the [`pg_cursors`](view-pg-cursors.md "52.6. pg_cursors") system view. 

If a cursor is closed after a savepoint which is later rolled back, the `CLOSE` is not rolled back; that is, the cursor remains closed. 

## Examples

Close the cursor `liahona`: 
    
    
    CLOSE liahona;
    

## Compatibility

`CLOSE` is fully conforming with the SQL standard. `CLOSE ALL` is a PostgreSQL extension. 

## See Also

[DECLARE](sql-declare.md "DECLARE"), [FETCH](sql-fetch.md "FETCH"), [MOVE](sql-move.md "MOVE")

* * *

[Prev](sql-checkpoint.md "CHECKPOINT") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-cluster.md "CLUSTER")  
---|---|---  
CHECKPOINT | [Home](index.md "PostgreSQL 17.5 Documentation")|  CLUSTER
