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
- : Add a new column to the table
- : Add column implicitly via CREATE OR REPLACE VIEW
- : Alter column default value
- : Add a pre-processed column default
- : Remove NOT NULL constraint from column
- : Add NOT NULL constraint to column
- : Set generated column expression
- : Remove generated column expression
- : Verify column is already NOT NULL
- : Set column statistics target
- : Set column options
- : Reset column options
- : Set column storage method
- : Set column compression method
- : Remove column from table
- : Change column data type
- : Modify column generic options

### Index and Constraint Operations:
- : Add an index
- : Internal re-addition of index
- : Add table constraint
- : Internal re-addition of constraint
- : Internal re-addition of domain constraint
- : Modify existing constraint
- : Validate constraint
- : Add constraint using existing index
- : Remove constraint

### Table Properties:
- : Change table owner
- : Set clustering index
- : Remove clustering
- : Convert to logged table
- : Convert to unlogged table
- : Remove OID support
- : Change table access method
- : Move to different tablespace
- : Set relation options
- : Reset relation options
- : Replace all relation options

### Trigger Management:
- : Enable specific trigger
- : Enable trigger in all modes
- : Enable trigger for replication
- : Disable specific trigger
- : Enable all triggers
- : Disable all triggers
- : Enable user triggers
- : Disable user triggers

### Rule Management:
- : Enable specific rule
- : Enable rule in all modes
- : Enable rule for replication
- : Disable specific rule

### Inheritance and Partitioning:
- : Add table inheritance
- : Remove table inheritance
- : Make table of composite type
- : Remove composite type association
- : Attach partition to partitioned table
- : Detach partition from partitioned table
- : Finalize partition detachment

### Security and Identity:
- : Set replica identity
- : Enable row-level security
- : Disable row-level security
- : Force row-level security for owner
- : Remove forced row-level security
- : Add identity column property
- : Modify identity column options
- : Remove identity column property

### Miscellaneous:
- : Internal comment re-addition
- : Set generic options
- : Internal statistics re-addition

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