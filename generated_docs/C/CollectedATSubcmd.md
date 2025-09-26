# CollectedATSubcmd

## Location
[src/include/tcop/deparse_utility.h:38-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tcop/deparse_utility.h#L38-L42)

## Overview
CollectedATSubcmd represents a single subcommand within an ALTER TABLE operation that is collected during event trigger processing for DDL command tracking and logging.

## Definition
```c
typedef struct CollectedATSubcmd
{
    ObjectAddress address;		/* affected column, constraint, index, ... */
    Node       *parsetree;
} CollectedATSubcmd;
```

## Detailed Description
CollectedATSubcmd is a structure used within PostgreSQL's event trigger system to capture individual subcommands that are part of an ALTER TABLE statement. When an ALTER TABLE command is executed, it may contain multiple suboperations (like adding a column, dropping a constraint, creating an index, etc.). Each of these suboperations is represented by a CollectedATSubcmd structure.

This structure is primarily used for:
1. Event trigger processing - allowing event triggers to inspect and react to individual ALTER TABLE subcommands
2. Command logging and auditing - providing detailed tracking of what specific objects were affected by each part of a complex ALTER TABLE operation
3. DDL deparsing - supporting the reconstruction of DDL commands for replication or logging purposes

The structure is created and populated in the EventTriggerCollectAlterTableSubcmd() function in event_trigger.c, which is called during the execution of ALTER TABLE subcommands to collect information about each individual operation within the larger ALTER TABLE command.

## Parameters / Member Variables
- `address`: An ObjectAddress structure that identifies the specific database object (column, constraint, index, etc.) that is affected by this subcommand
- `parsetree`: A pointer to the parse tree Node representing the specific subcommand within the ALTER TABLE statement

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAddress (from catalog/objectaddress.h)
  - Node (from nodes/nodes.h)
- Called from (representative examples):
  - EventTriggerCollectAlterTableSubcmd()
  - Used within CollectedCommand.d.alterTable.subcmds list

## Notes and Other Information
- This structure is allocated in the event trigger memory context to ensure it persists for the duration of the DDL command execution
- The parsetree is copied using copyObject() to ensure the data remains valid after the original parse tree is freed
- These structures are collected into a list that is stored in the CollectedCommand.d.alterTable.subcmds field for ALTER TABLE operations
- The structure is specifically designed for ALTER TABLE commands and is not used for other types of DDL operations
- Part of the broader event trigger infrastructure that enables extensions and custom code to monitor and react to DDL changes