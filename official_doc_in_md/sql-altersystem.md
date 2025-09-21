ALTER SYSTEM  
---  
[Prev](sql-altersubscription.md "ALTER SUBSCRIPTION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-altertable.md "ALTER TABLE")  
  
* * *

## ALTER SYSTEM

ALTER SYSTEM — change a server configuration parameter

## Synopsis
    
    
    ALTER SYSTEM SET _configuration_parameter_ { TO | = } { _value_ [, ...] | DEFAULT }
    
    ALTER SYSTEM RESET _configuration_parameter_
    ALTER SYSTEM RESET ALL
    

## Description

`ALTER SYSTEM` is used for changing server configuration parameters across the entire database cluster. It can be more convenient than the traditional method of manually editing the `postgresql.conf` file. `ALTER SYSTEM` writes the given parameter setting to the `postgresql.auto.conf` file, which is read in addition to `postgresql.conf`. Setting a parameter to `DEFAULT`, or using the `RESET` variant, removes that configuration entry from the `postgresql.auto.conf` file. Use `RESET ALL` to remove all such configuration entries. 

Values set with `ALTER SYSTEM` will be effective after the next server configuration reload, or after the next server restart in the case of parameters that can only be changed at server start. A server configuration reload can be commanded by calling the SQL function `pg_reload_conf()`, running `pg_ctl reload`, or sending a SIGHUP signal to the main server process. 

Only superusers and users granted `ALTER SYSTEM` privilege on a parameter can change it using `ALTER SYSTEM`. Also, since this command acts directly on the file system and cannot be rolled back, it is not allowed inside a transaction block or function. 

## Parameters

 _`configuration_parameter`_
    

Name of a settable configuration parameter. Available parameters are documented in [Chapter 19](runtime-config.md "Chapter 19. Server Configuration"). 

_`value`_
    

New value of the parameter. Values can be specified as string constants, identifiers, numbers, or comma-separated lists of these, as appropriate for the particular parameter. Values that are neither numbers nor valid identifiers must be quoted. `DEFAULT` can be written to specify removing the parameter and its value from `postgresql.auto.conf`. 

For some list-accepting parameters, quoted values will produce double-quoted output to preserve whitespace and commas; for others, double-quotes must be used inside single-quoted strings to get this effect. 

## Notes

This command can't be used to set [data_directory](runtime-config-file-locations.md#GUC-DATA-DIRECTORY), [allow_alter_system](runtime-config-compatible.md#GUC-ALLOW-ALTER-SYSTEM), nor parameters that are not allowed in `postgresql.conf` (e.g., [preset options](runtime-config-preset.md "19.15. Preset Options")). 

See [Section 19.1](config-setting.md "19.1. Setting Parameters") for other ways to set the parameters. 

`ALTER SYSTEM` can be disabled by setting [allow_alter_system](runtime-config-compatible.md#GUC-ALLOW-ALTER-SYSTEM) to `off`, but this is not a security mechanism (as explained in detail in the documentation for this parameter). 

## Examples

Set the `wal_level`: 
    
    
    ALTER SYSTEM SET wal_level = replica;
    

Undo that, restoring whatever setting was effective in `postgresql.conf`: 
    
    
    ALTER SYSTEM RESET wal_level;
    

## Compatibility

The `ALTER SYSTEM` statement is a PostgreSQL extension. 

## See Also

[SET](sql-set.md "SET"), [SHOW](sql-show.md "SHOW")

* * *

[Prev](sql-altersubscription.md "ALTER SUBSCRIPTION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-altertable.md "ALTER TABLE")  
---|---|---  
ALTER SUBSCRIPTION | [Home](index.md "PostgreSQL 17.5 Documentation")|  ALTER TABLE
