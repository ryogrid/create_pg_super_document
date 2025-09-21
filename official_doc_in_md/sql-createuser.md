CREATE USER  
---  
[Prev](sql-createtype.md "CREATE TYPE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-createusermapping.md "CREATE USER MAPPING")  
  
* * *

## CREATE USER

CREATE USER — define a new database role

## Synopsis
    
    
    CREATE USER _name_ [ [ WITH ] _option_ [ ... ] ]
    
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

`CREATE USER` is now an alias for [`CREATE ROLE`](sql-createrole.md "CREATE ROLE"). The only difference is that when the command is spelled `CREATE USER`, `LOGIN` is assumed by default, whereas `NOLOGIN` is assumed when the command is spelled `CREATE ROLE`. 

## Compatibility

The `CREATE USER` statement is a PostgreSQL extension. The SQL standard leaves the definition of users to the implementation. 

## See Also

[CREATE ROLE](sql-createrole.md "CREATE ROLE")

* * *

[Prev](sql-createtype.md "CREATE TYPE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-createusermapping.md "CREATE USER MAPPING")  
---|---|---  
CREATE TYPE | [Home](index.md "PostgreSQL 17.5 Documentation")|  CREATE USER MAPPING
