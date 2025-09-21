ALTER USER  
---  
[Prev](sql-altertype.md "ALTER TYPE") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-alterusermapping.md "ALTER USER MAPPING")  
  
* * *

## ALTER USER

ALTER USER — change a database role

## Synopsis
    
    
    ALTER USER _role_specification_ [ WITH ] _option_ [ ... ]
    
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
    
    ALTER USER _name_ RENAME TO _new_name_
    
    ALTER USER { _role_specification_ | ALL } [ IN DATABASE _database_name_ ] SET _configuration_parameter_ { TO | = } { _value_ | DEFAULT }
    ALTER USER { _role_specification_ | ALL } [ IN DATABASE _database_name_ ] SET _configuration_parameter_ FROM CURRENT
    ALTER USER { _role_specification_ | ALL } [ IN DATABASE _database_name_ ] RESET _configuration_parameter_
    ALTER USER { _role_specification_ | ALL } [ IN DATABASE _database_name_ ] RESET ALL
    
    where _role_specification_ can be:
    
        _role_name_
      | CURRENT_ROLE
      | CURRENT_USER
      | SESSION_USER
    

## Description

`ALTER USER` is now an alias for [`ALTER ROLE`](sql-alterrole.md "ALTER ROLE"). 

## Compatibility

The `ALTER USER` statement is a PostgreSQL extension. The SQL standard leaves the definition of users to the implementation. 

## See Also

[ALTER ROLE](sql-alterrole.md "ALTER ROLE")

* * *

[Prev](sql-altertype.md "ALTER TYPE") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-alterusermapping.md "ALTER USER MAPPING")  
---|---|---  
ALTER TYPE | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER USER MAPPING
