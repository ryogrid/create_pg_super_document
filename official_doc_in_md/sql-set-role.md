SET ROLE  
---  
[Prev](sql-set-constraints.md "SET CONSTRAINTS") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-set-session-authorization.md "SET SESSION AUTHORIZATION")  
  
* * *

## SET ROLE

SET ROLE — set the current user identifier of the current session

## Synopsis
    
    
    SET [ SESSION | LOCAL ] ROLE _role_name_
    SET [ SESSION | LOCAL ] ROLE NONE
    RESET ROLE
    

## Description

This command sets the current user identifier of the current SQL session to be _`role_name`_. The role name can be written as either an identifier or a string literal. After `SET ROLE`, permissions checking for SQL commands is carried out as though the named role were the one that had logged in originally. Note that `SET ROLE` and `SET SESSION AUTHORIZATION` are exceptions; permissions checks for those continue to use the current session user and the initial session user (the _authenticated user_), respectively. 

The current session user must have the `SET` option for the specified _`role_name`_ , either directly or indirectly via a chain of memberships with the `SET` option. (If the session user is a superuser, any role can be selected.) 

The `SESSION` and `LOCAL` modifiers act the same as for the regular [`SET`](sql-set.md "SET") command. 

`SET ROLE NONE` sets the current user identifier to the current session user identifier, as returned by `session_user`. `RESET ROLE` sets the current user identifier to the connection-time setting specified by the [command-line options](libpq-connect.md#LIBPQ-CONNECT-OPTIONS), [`ALTER ROLE`](sql-alterrole.md "ALTER ROLE"), or [`ALTER DATABASE`](sql-alterdatabase.md "ALTER DATABASE"), if any such settings exist. Otherwise, `RESET ROLE` sets the current user identifier to the current session user identifier. These forms can be executed by any user. 

## Notes

Using this command, it is possible to either add privileges or restrict one's privileges. If the session user role has been granted memberships `WITH INHERIT TRUE`, it automatically has all the privileges of every such role. In this case, `SET ROLE` effectively drops all the privileges except for those which the target role directly possesses or inherits. On the other hand, if the session user role has been granted memberships `WITH INHERIT FALSE`, the privileges of the granted roles can't be accessed by default. However, if the role was granted `WITH SET TRUE`, the session user can use `SET ROLE` to drop the privileges assigned directly to the session user and instead acquire the privileges available to the named role. If the role was granted `WITH INHERIT FALSE, SET FALSE` then the privileges of that role cannot be exercised either with or without `SET ROLE`. 

`SET ROLE` has effects comparable to [`SET SESSION AUTHORIZATION`](sql-set-session-authorization.md "SET SESSION AUTHORIZATION"), but the privilege checks involved are quite different. Also, `SET SESSION AUTHORIZATION` determines which roles are allowable for later `SET ROLE` commands, whereas changing roles with `SET ROLE` does not change the set of roles allowed to a later `SET ROLE`. 

`SET ROLE` does not process session variables as specified by the role's [`ALTER ROLE`](sql-alterrole.md "ALTER ROLE") settings; this only happens during login. 

`SET ROLE` cannot be used within a `SECURITY DEFINER` function. 

## Examples
    
    
    SELECT SESSION_USER, CURRENT_USER;
    
     session_user | current_user
    --------------+--------------
     peter        | peter
    
    SET ROLE 'paul';
    
    SELECT SESSION_USER, CURRENT_USER;
    
     session_user | current_user
    --------------+--------------
     peter        | paul
    

## Compatibility

PostgreSQL allows identifier syntax (`"_`rolename`_ "`), while the SQL standard requires the role name to be written as a string literal. SQL does not allow this command during a transaction; PostgreSQL does not make this restriction because there is no reason to. The `SESSION` and `LOCAL` modifiers are a PostgreSQL extension, as is the `RESET` syntax. 

## See Also

[SET SESSION AUTHORIZATION](sql-set-session-authorization.md "SET SESSION AUTHORIZATION")

* * *

[Prev](sql-set-constraints.md "SET CONSTRAINTS") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-set-session-authorization.md "SET SESSION AUTHORIZATION")  
---|---|---  
SET CONSTRAINTS | [Home](index.md "PostgreSQL 17.5 Documentation")|  SET SESSION AUTHORIZATION
