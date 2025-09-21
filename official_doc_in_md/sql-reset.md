RESET  
---  
[Prev](sql-release-savepoint.md "RELEASE SAVEPOINT") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-revoke.md "REVOKE")  
  
* * *

## RESET

RESET — restore the value of a run-time parameter to the default value

## Synopsis
    
    
    RESET _configuration_parameter_
    RESET ALL
    

## Description

`RESET` restores run-time parameters to their default values. `RESET` is an alternative spelling for 
    
    
    SET _configuration_parameter_ TO DEFAULT
    

Refer to [SET](sql-set.md "SET") for details. 

The default value is defined as the value that the parameter would have had, if no `SET` had ever been issued for it in the current session. The actual source of this value might be a compiled-in default, the configuration file, command-line options, or per-database or per-user default settings. This is subtly different from defining it as “the value that the parameter had at session start”, because if the value came from the configuration file, it will be reset to whatever is specified by the configuration file now. See [Chapter 19](runtime-config.md "Chapter 19. Server Configuration") for details. 

The transactional behavior of `RESET` is the same as `SET`: its effects will be undone by transaction rollback. 

## Parameters

 _`configuration_parameter`_
    

Name of a settable run-time parameter. Available parameters are documented in [Chapter 19](runtime-config.md "Chapter 19. Server Configuration") and on the [SET](sql-set.md "SET") reference page. 

`ALL`
    

Resets all settable run-time parameters to default values. 

## Examples

Set the `timezone` configuration variable to its default value: 
    
    
    RESET timezone;
    

## Compatibility

`RESET` is a PostgreSQL extension. 

## See Also

[SET](sql-set.md "SET"), [SHOW](sql-show.md "SHOW")

* * *

[Prev](sql-release-savepoint.md "RELEASE SAVEPOINT") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-revoke.md "REVOKE")  
---|---|---  
RELEASE SAVEPOINT | [Home](index.md "PostgreSQL 17.5 Documentation")|  REVOKE
