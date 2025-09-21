CREATE GROUP  
---  
[Prev](sql-createfunction.md "CREATE FUNCTION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-createindex.md "CREATE INDEX")  
  
* * *

## CREATE GROUP

CREATE GROUP — define a new database role

## Synopsis
    
    
    CREATE GROUP _name_ [ [ WITH ] _option_ [ ... ] ]
    
    where _option_ can be:
    
          SUPERUSER | NOSUPERUSER
        | CREATEDB | NOCREATEDB
        | CREATEROLE | NOCREATEROLE
        | INHERIT | NOINHERIT
        | LOGIN | NOLOGIN
        | REPLICATION | NOREPLICATION
        | BYPASSRLS | NOBYPASSRLS
        | CONNECTION LIMIT _connlimit_
        | [ ENCRYPTED ] PASSWORD '_password_ ' | PASSWORD NULL
        | VALID UNTIL '_timestamp_ '
        | IN ROLE _role_name_ [, ...]
        | IN GROUP _role_name_ [, ...]
        | ROLE _role_name_ [, ...]
        | ADMIN _role_name_ [, ...]
        | USER _role_name_ [, ...]
        | SYSID _uid_
    

## Description

`CREATE GROUP` is now an alias for [CREATE ROLE](sql-createrole.md "CREATE ROLE"). 

## Compatibility

There is no `CREATE GROUP` statement in the SQL standard. 

## See Also

[CREATE ROLE](sql-createrole.md "CREATE ROLE")

* * *

[Prev](sql-createfunction.md "CREATE FUNCTION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-createindex.md "CREATE INDEX")  
---|---|---  
CREATE FUNCTION | [Home](index.md "PostgreSQL 17.5 Documentation")|  CREATE INDEX
