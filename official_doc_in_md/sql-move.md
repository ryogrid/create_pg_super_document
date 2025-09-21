MOVE  
---  
[Prev](sql-merge.md "MERGE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-notify.md "NOTIFY")  
  
* * *

## MOVE

MOVE — position a cursor

## Synopsis
    
    
    MOVE [ _direction_ ] [ FROM | IN ] _cursor_name_
    
    where _direction_ can be one of:
    
        NEXT
        PRIOR
        FIRST
        LAST
        ABSOLUTE _count_
        RELATIVE _count_
        _count_
        ALL
        FORWARD
        FORWARD _count_
        FORWARD ALL
        BACKWARD
        BACKWARD _count_
        BACKWARD ALL
    

## Description

`MOVE` repositions a cursor without retrieving any data. `MOVE` works exactly like the `FETCH` command, except it only positions the cursor and does not return rows. 

The parameters for the `MOVE` command are identical to those of the `FETCH` command; refer to [FETCH](sql-fetch.md "FETCH") for details on syntax and usage. 

## Outputs

On successful completion, a `MOVE` command returns a command tag of the form 
    
    
    MOVE _count_
    

The _`count`_ is the number of rows that a `FETCH` command with the same parameters would have returned (possibly zero). 

## Examples
    
    
    BEGIN WORK;
    DECLARE liahona CURSOR FOR SELECT * FROM films;
    
    -- Skip the first 5 rows:
    MOVE FORWARD 5 IN liahona;
    MOVE 5
    
    -- Fetch the 6th row from the cursor liahona:
    FETCH 1 FROM liahona;
     code  | title  | did | date_prod  |  kind  |  len
    -------+--------+-----+------------+--------+-------
     P_303 | 48 Hrs | 103 | 1982-10-22 | Action | 01:37
    (1 row)
    
    -- Close the cursor liahona and end the transaction:
    CLOSE liahona;
    COMMIT WORK;
    

## Compatibility

There is no `MOVE` statement in the SQL standard. 

## See Also

[CLOSE](sql-close.md "CLOSE"), [DECLARE](sql-declare.md "DECLARE"), [FETCH](sql-fetch.md "FETCH")

* * *

[Prev](sql-merge.md "MERGE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-notify.md "NOTIFY")  
---|---|---  
MERGE | [Home](index.md "PostgreSQL 17.5 Documentation")|  NOTIFY
