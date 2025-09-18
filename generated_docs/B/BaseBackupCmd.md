# BaseBackupCmd

## Location
src/include/nodes/replnodes.h: 41 - 45

## Overview
BaseBackupCmd represents the BASE_BACKUP replication protocol command, used to initiate a physical backup of a PostgreSQL database cluster with configurable options.

## Definition
```c
typedef struct BaseBackupCmd
{
    NodeTag     type;
    List       *options;
} BaseBackupCmd;
```

## Detailed Description
BaseBackupCmd is a structure that encapsulates the BASE_BACKUP replication command. This command initiates a physical (binary) backup of the entire PostgreSQL database cluster, streaming the backup data to the requesting client. The structure contains a list of options that control various aspects of the backup process.

When processed by the walsender, this command triggers the SendBaseBackup() function which handles the actual backup process. The backup creates a consistent snapshot of the database cluster that can be used to restore or set up streaming replication standbys.

The options list contains DefElem structures that specify various backup parameters such as:
- Label for the backup
- Progress reporting
- Checkpoint behavior
- WAL inclusion/exclusion
- Compression settings
- Manifest generation
- Incremental backup support
- And many other backup configuration options

## Parameters / Member Variables
- `type`: NodeTag identifying this as a T_BaseBackupCmd node type
- `options`: List of DefElem structures containing backup configuration options (label, progress, checkpoint, wal, compression, manifest, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (from nodes/nodes.h)
  - [List](../L/List.md) (from nodes/pg_list.h)
- Called from (representative examples):
  - walsender.c:2124 - [SendBaseBackup](../S/SendBaseBackup.md)((BaseBackupCmd *) cmd_node, uploaded_manifest)
  - Processed in replication command switch statement at walsender.c:2120

## Notes and Other Information
- Essential component of PostgreSQL's physical backup and replication infrastructure  
- Supports both full and incremental backup modes
- The backup process prevents concurrent transactions during execution
- Options are parsed by parse_basebackup_options() to populate a basebackup_options struct
- Used by tools like pg_basebackup to create database backups
- Part of the PostgreSQL streaming replication protocol
- Located in src/include/nodes/replnodes.h alongside other replication command structures