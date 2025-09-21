SHOW  
---  
[Prev](sql-set-transaction.md "SET TRANSACTION") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-start-transaction.md "START TRANSACTION")  
  
* * *

## SHOW

SHOW — show the value of a run-time parameter

## Synopsis
    
    
    SHOW _name_
    SHOW ALL
    

## Description

`SHOW` will display the current setting of run-time parameters. These variables can be set using the `SET` statement, by editing the `postgresql.conf` configuration file, through the `PGOPTIONS` environmental variable (when using libpq or a libpq-based application), or through command-line flags when starting the `postgres` server. See [Chapter 19](runtime-config.md "Chapter 19. Server Configuration") for details. 

## Parameters

 _`name`_
    

The name of a run-time parameter. Available parameters are documented in [Chapter 19](runtime-config.md "Chapter 19. Server Configuration") and on the [SET](sql-set.md "SET") reference page. In addition, there are a few parameters that can be shown but not set: 

`SERVER_VERSION`
    

Shows the server's version number. 

`SERVER_ENCODING`
    

Shows the server-side character set encoding. At present, this parameter can be shown but not set, because the encoding is determined at database creation time. 

`IS_SUPERUSER`
    

True if the current role has superuser privileges. 

`ALL`
    

Show the values of all configuration parameters, with descriptions. 

## Notes

The function `current_setting` produces equivalent output; see [Section 9.28.1](functions-admin.md#FUNCTIONS-ADMIN-SET "9.28.1. Configuration Settings Functions"). Also, the [`pg_settings`](view-pg-settings.md "52.24. pg_settings") system view produces the same information. 

## Examples

Show the current setting of the parameter `DateStyle`: 
    
    
    SHOW DateStyle;
     DateStyle
    -----------
     ISO, MDY
    (1 row)
    

Show the current setting of the parameter `geqo`: 
    
    
    SHOW geqo;
     geqo
    ------
     on
    (1 row)
    

Show all settings: 
    
    
    SHOW ALL;
                name         | setting |                description
    -------------------------+---------+-------------------------------------------------
     allow_system_table_mods | off     | Allows modifications of the structure of ...
        .
        .
        .
     xmloption               | content | Sets whether XML data in implicit parsing ...
     zero_damaged_pages      | off     | Continues processing past damaged page headers.
    (196 rows)
    

## Compatibility

The `SHOW` command is a PostgreSQL extension. 

## See Also

[SET](sql-set.md "SET"), [RESET](sql-reset.md "RESET")

* * *

[Prev](sql-set-transaction.md "SET TRANSACTION") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-start-transaction.md "START TRANSACTION")  
---|---|---  
SET TRANSACTION | [Home](index.md "PostgreSQL 17.5 Documentation")|  START TRANSACTION
