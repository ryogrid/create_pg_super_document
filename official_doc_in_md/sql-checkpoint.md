CHECKPOINT  
---  
[Prev](sql-call.md "CALL") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-close.md "CLOSE")  
  
* * *

## CHECKPOINT

CHECKPOINT — force a write-ahead log checkpoint

## Synopsis
    
    
    CHECKPOINT
    

## Description

A checkpoint is a point in the write-ahead log sequence at which all data files have been updated to reflect the information in the log. All data files will be flushed to disk. Refer to [Section 28.5](wal-configuration.md "28.5. WAL Configuration") for more details about what happens during a checkpoint. 

The `CHECKPOINT` command forces an immediate checkpoint when the command is issued, without waiting for a regular checkpoint scheduled by the system (controlled by the settings in [Section 19.5.2](runtime-config-wal.md#RUNTIME-CONFIG-WAL-CHECKPOINTS "19.5.2. Checkpoints")). `CHECKPOINT` is not intended for use during normal operation. 

If executed during recovery, the `CHECKPOINT` command will force a restartpoint (see [Section 28.5](wal-configuration.md "28.5. WAL Configuration")) rather than writing a new checkpoint. 

Only superusers or users with the privileges of the [`pg_checkpoint`](predefined-roles.md#PREDEFINED-ROLES-TABLE "Table 21.1. Predefined Roles") role can call `CHECKPOINT`. 

## Compatibility

The `CHECKPOINT` command is a PostgreSQL language extension. 

* * *

[Prev](sql-call.md "CALL") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-close.md "CLOSE")  
---|---|---  
CALL | [Home](index.md "PostgreSQL 17.5 Documentation")|  CLOSE
