# AlterTableType

## Location
[src/include/nodes/parsenodes.h:2417-2418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2417-L2418)

## Overview
AlterTableType is an enumeration that defines the various types of operations that can be performed on a table through the ALTER TABLE command in PostgreSQL.

## Definition

```c
typedef struct ReplicaIdentityStmt
{
	NodeTag		type;
	char		identity_type;
	char	   *name;
} ReplicaIdentityStmt;
```
## Detailed Description
This enumeration serves as a comprehensive catalog of all possible ALTER TABLE operations in PostgreSQL. Each enum value corresponds to a specific type of table modification that can be requested through SQL DDL commands. The enum is used primarily by the ALTER TABLE command processing infrastructure to identify and route different types of table alterations to their appropriate handler functions.

The enum values cover a wide range of operations including column management (add, drop, modify), constraint management, index operations, trigger and rule management, inheritance operations, partitioning, security settings, and various table properties.

## Parameters / Member Variables
The enum values can be categorized as follows:

### Column Operations:
- `AT_AddColumn`: Add a new column to the table
- `AT_AddColumnToView`: Add column implicitly via CREATE OR REPLACE VIEW
- `AT_ColumnDefault`: Alter column default value
- `AT_CookedColumnDefault`: Add a pre-processed column default
- `AT_DropNotNull`: Remove NOT NULL constraint from column
- `AT_SetNotNull`: Add NOT NULL constraint to column
- `AT_SetIdentity`: Set generated column expression
- `AT_DropIdentity`: Remove generated column expression
- `AT_CheckNotNull`: Verify column is already NOT NULL
- `AT_SetStatistics`: Set column statistics target
- `AT_SetOptions`: Set column options
- `AT_ResetOptions`: Reset column options
- `AT_SetStorage`: Set column storage method
- `AT_SetCompression`: Set column compression method
- `AT_DropColumn`: Remove column from table
- `AT_AlterColumnType`: Change column data type
- `AT_AlterColumnGenericOptions`: Modify column generic options

### Index and Constraint Operations:
- `AT_AddIndex`: Add an index
- `AT_ReAddIndex`: Internal re-addition of index
- `AT_AddConstraint`: Add table constraint
- `AT_ReAddConstraint`: Internal re-addition of constraint
- `AT_ReAddDomainConstraint`: Internal re-addition of domain constraint
- `AT_AlterConstraint`: Modify existing constraint
- `AT_ValidateConstraint`: Validate constraint
- `AT_AddIndexConstraint`: Add constraint using existing index
- `AT_DropConstraint`: Remove constraint

### Table Properties:
- `AT_ChangeOwner`: Change table owner
- `AT_ClusterOn`: Set clustering index
- `AT_DropCluster`: Remove clustering
- `AT_SetLogged`: Convert to logged table
- `AT_SetUnLogged`: Convert to unlogged table
- `AT_DropOids`: Remove OID support
- `AT_SetAccessMethod`: Change table access method
- `AT_SetTableSpace`: Move to different tablespace
- `AT_SetRelOptions`: Set relation options
- `AT_ResetRelOptions`: Reset relation options
- `AT_ReplaceRelOptions`: Replace all relation options

### Trigger Management:
- `AT_EnableTrig`: Enable specific trigger
- `AT_EnableAlwaysTrig`: Enable trigger in all modes
- `AT_EnableReplicaTrig`: Enable trigger for replication
- `AT_DisableTrig`: Disable specific trigger
- `AT_EnableTrigAll`: Enable all triggers
- `AT_DisableTrigAll`: Disable all triggers
- `AT_EnableTrigUser`: Enable user triggers
- `AT_DisableTrigUser`: Disable user triggers

### Rule Management:
- `AT_EnableRule`: Enable specific rule
- `AT_EnableAlwaysRule`: Enable rule in all modes
- `AT_EnableReplicaRule`: Enable rule for replication
- `AT_DisableRule`: Disable specific rule

### Inheritance and Partitioning:
- `AT_AddInherit`: Add table inheritance
- `AT_DropInherit`: Remove table inheritance
- `AT_AddOf`: Make table of composite type
- `AT_DropOf`: Remove composite type association
- `AT_AttachPartition`: Attach partition to partitioned table
- `AT_DetachPartition`: Detach partition from partitioned table
- `AT_DetachPartitionFinalize`: Finalize partition detachment

### Security and Identity:
- `AT_ReplicaIdentity`: Set replica identity
- `AT_EnableRowSecurity`: Enable row-level security
- `AT_DisableRowSecurity`: Disable row-level security
- `AT_ForceRowSecurity`: Force row-level security for owner
- `AT_NoForceRowSecurity`: Remove forced row-level security
- `AT_AddIdentity`: Add identity column property
- `AT_SetIdentity`: Modify identity column options
- `AT_DropIdentity`: Remove identity column property

### Miscellaneous:
- `AT_ReAddComment`: Internal comment re-addition
- `AT_GenericOptions`: Set generic options
- `AT_ReAddStatistics`: Internal statistics re-addition

## Dependencies
- Functions called/Symbols referenced: None (this is an enum definition)
- Called from (representative examples):
  -  in src/backend/commands/tablecmds.c:432
  -  in src/backend/commands/tablecmds.c:6398
  -  in src/backend/commands/tablecmds.c:6543
  -  structure in src/include/nodes/parsenodes.h:2429

## Notes and Other Information
- This enum is defined in src/include/nodes/parsenodes.h:2348-2417
- Several enum values are marked as "internal to commands/tablecmds.c", indicating they are used for internal processing during complex table alterations
- The enum is primarily used in the  structure to specify the type of alteration being performed
- The comprehensive nature of this enum reflects PostgreSQL's extensive ALTER TABLE functionality
- Some operations may require specific permissions or have restrictions based on table type or current state