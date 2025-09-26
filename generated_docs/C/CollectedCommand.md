# CollectedCommand

## Location
src/include/tcop/deparse_utility.h: 44 - 106

## Overview
CollectedCommand is a comprehensive structure that represents a DDL (Data Definition Language) command collected during event trigger processing, supporting various command types through a discriminated union design.

## Definition
```c
typedef struct CollectedCommand
{
    CollectedCommandType type;

    bool        in_extension;
    Node       *parsetree;

    union
    {
        /* most commands */
        struct
        {
            ObjectAddress address;
            ObjectAddress secondaryObject;
        }           simple;

        /* ALTER TABLE, and internal uses thereof */
        struct
        {
            Oid         objectId;
            Oid         classId;
            List       *subcmds;
        }           alterTable;

        /* GRANT / REVOKE */
        struct
        {
            InternalGrant *istmt;
        }           grant;

        /* ALTER OPERATOR FAMILY */
        struct
        {
            ObjectAddress address;
            List       *operators;
            List       *procedures;
        }           opfam;

        /* CREATE OPERATOR CLASS */
        struct
        {
            ObjectAddress address;
            List       *operators;
            List       *procedures;
        }           createopc;

        /* ALTER TEXT SEARCH CONFIGURATION ADD/ALTER/DROP MAPPING */
        struct
        {
            ObjectAddress address;
            Oid        *dictIds;
            int         ndicts;
        }           atscfg;

        /* ALTER DEFAULT PRIVILEGES */
        struct
        {
            ObjectType  objtype;
        }           defprivs;
    }           d;

    struct CollectedCommand *parent;    /* when nested */
} CollectedCommand;
```

## Detailed Description
CollectedCommand is the central data structure in PostgreSQL's event trigger system for tracking DDL commands. It provides a unified representation for different types of DDL operations while maintaining specific information relevant to each command type through a discriminated union.

The structure supports the following main purposes:
1. **Event Trigger Processing**: Enables event triggers to inspect and react to various DDL commands with detailed command-specific information
2. **Command Logging**: Provides comprehensive tracking of DDL operations for auditing, replication, and monitoring
3. **DDL Deparsing**: Supports reconstruction of DDL commands from their collected representation
4. **Extension Support**: Tracks whether commands are executed within extensions for proper handling

The discriminated union design allows efficient storage of command-specific data while maintaining type safety. Each command type (identified by the `type` field) determines which member of the union contains valid data.

## Parameters / Member Variables
- `type`: A CollectedCommandType enum value indicating which specific DDL command type this structure represents
- `in_extension`: Boolean flag indicating whether this command was executed as part of an extension installation
- `parsetree`: Pointer to the original parse tree Node for the DDL command
- `d`: Discriminated union containing command-specific data:
  - `simple`: For most basic DDL commands with primary and optional secondary object addresses
  - `alterTable`: For ALTER TABLE commands with object identifiers and list of subcommands (CollectedATSubcmd)
  - `grant`: For GRANT/REVOKE commands containing an InternalGrant statement
  - `opfam`: For ALTER OPERATOR FAMILY commands with address and lists of operators/procedures
  - `createopc`: For CREATE OPERATOR CLASS commands with address and lists of operators/procedures  
  - `atscfg`: For ALTER TEXT SEARCH CONFIGURATION commands with address and dictionary information
  - `defprivs`: For ALTER DEFAULT PRIVILEGES commands with object type information
- `parent`: Pointer to parent CollectedCommand when commands are nested (supports hierarchical command structures)

## Dependencies
- Functions called/Symbols referenced:
  - CollectedCommandType (enum)
  - ObjectAddress (from catalog/objectaddress.h)
  - Node (from nodes/nodes.h)
  - InternalGrant (from utils/aclchk_internal.h)
  - CollectedATSubcmd
- Called from (representative examples):
  - EventTriggerCollectSimpleCommand()
  - EventTriggerAlterTableStart()
  - EventTriggerCollectGrant()
  - EventTriggerCollectAlterOpFamily()
  - EventTriggerCollectCreateOpClass()
  - EventTriggerCollectAlterTSConfig()
  - EventTriggerCollectAlterDefPrivs()

## Notes and Other Information
- The structure is allocated in the event trigger memory context to ensure persistence during DDL command execution
- Commands are collected in a list stored in EventTriggerQueryState.commandList for batch processing
- The union design ensures memory efficiency while supporting diverse command types
- The parent field enables tracking of nested commands, such as when ALTER TABLE operations contain multiple subcommands
- Used extensively in the DDL event trigger infrastructure, particularly for the ddl_command_start and ddl_command_end events
- The structure supports extension of new DDL command types by adding new CollectedCommandType values and corresponding union members
- Critical for logical replication, auditing systems, and any extensions that need to monitor or react to DDL changes