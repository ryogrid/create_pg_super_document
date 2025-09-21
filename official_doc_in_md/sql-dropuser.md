DROP USER  
---  
[Prev](sql-droptype.md "DROP TYPE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-dropusermapping.md "DROP USER MAPPING")  
  
* * *

## DROP USER

DROP USER — remove a database role

## Synopsis
    
    
    DROP USER [ IF EXISTS ] _name_ [, ...]
    

## Description

`DROP USER` is simply an alternate spelling of [`DROP ROLE`](sql-droprole.md "DROP ROLE"). 

## Compatibility

The `DROP USER` statement is a PostgreSQL extension. The SQL standard leaves the definition of users to the implementation. 

## See Also

[DROP ROLE](sql-droprole.md "DROP ROLE")

* * *

[Prev](sql-droptype.md "DROP TYPE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-dropusermapping.md "DROP USER MAPPING")  
---|---|---  
DROP TYPE | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP USER MAPPING
