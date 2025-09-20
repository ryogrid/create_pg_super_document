# ATGetQueueEntry

## Location
[src/backend/commands/tablecmds.c:6364-6397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L6364-L6397)

## Overview
ATGetQueueEntry finds an existing or creates a new AlteredTableInfo entry in the ALTER TABLE work queue for a specified relation.

## Definition

```c
static AlteredTableInfo *
ATGetQueueEntry(List **wqueue, Relation rel)
```
## Detailed Description
ATGetQueueEntry serves as the central registry mechanism for managing table alterations during complex ALTER TABLE operations. The function maintains a work queue (wqueue) that tracks all tables that need processing during an ALTER TABLE command, including the primary table being altered and any related tables that require cascading changes (such as child tables in inheritance hierarchies or tables with foreign key relationships).

The function first searches the existing work queue to see if an entry for the specified relation already exists, returning the existing entry if found. If no entry exists, it creates a new AlteredTableInfo structure, initializes it with the relation's current metadata, and adds it to the work queue. The initialization process captures a snapshot of the table's current tuple descriptor using CreateTupleDescCopyConstr, ensuring that the original schema definition is preserved for reference during the alteration process.

The function initializes several important fields in the AlteredTableInfo structure, setting default values for access method changes, tablespace changes, and persistence changes. These fields will be modified later during the ALTER TABLE processing as specific alterations are identified and planned.

## Parameters / Member Variables
- : Double pointer to the work queue list that maintains all AlteredTableInfo entries for the current ALTER TABLE operation
- : Relation pointer to the table for which a queue entry is needed

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelid
  - lfirst
  - [palloc0](../p/palloc0.md)
  - [CreateTupleDescCopyConstr](../C/CreateTupleDescCopyConstr.md)
  - RelationGetDescr
  - lappend
  - InvalidOid (constant)
  - RELPERSISTENCE_PERMANENT (constant)
- Called from:
  - [ATPrepCmd](ATPrepCmd.md)
  - [ATExecAddColumn](ATExecAddColumn.md)
  - [ATAddCheckConstraint](ATAddCheckConstraint.md)
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md)
  - [ATExecValidateConstraint](ATExecValidateConstraint.md)
  - [ATPostAlterTypeParse](ATPostAlterTypeParse.md)
  - [QueuePartitionConstraintValidation](../Q/QueuePartitionConstraintValidation.md)
  - [DetachAddConstraintIfNeeded](../D/DetachAddConstraintIfNeeded.md)

## Notes and Other Information
- This function is static and only used within the tablecmds.c module
- Returns a pointer to the AlteredTableInfo entry, either existing or newly created
- The function implements a simple linear search through the work queue to find existing entries
- Creates a deep copy of the relation's tuple descriptor to preserve the original schema definition
- Initializes new entries with conservative default values that can be modified later in the ALTER TABLE process
- The rel field in AlteredTableInfo is set to NULL initially and populated later during processing
- Part of PostgreSQL's multi-pass ALTER TABLE processing framework that handles complex interdependencies
- Critical for ensuring that all affected tables are properly tracked and processed during ALTER TABLE operations
- Located at src/backend/commands/tablecmds.c:6364-6397