# CreateTriggerFiringOn

## Location
[src/backend/commands/trigger.c:176-1215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L176-L1215)

## Overview
CreateTriggerFiringOn is the core PostgreSQL function that implements trigger creation with support for custom firing conditions, handling all the complex validation, catalog operations, and partition recursion.

## Definition
```c
ObjectAddress CreateTriggerFiringOn(CreateTrigStmt *stmt, const char *queryString,
                                   Oid relOid, Oid refRelOid, Oid constraintOid,
                                   Oid indexOid, Oid funcoid, Oid parentTriggerOid,
                                   Node *whenClause, bool isInternal, bool in_partition,
                                   char trigger_fires_when)
```

## Detailed Description
CreateTriggerFiringOn is the comprehensive implementation of trigger creation in PostgreSQL. It performs extensive validation of trigger properties against relation types, handles permission checks, validates the trigger function, processes WHEN clauses, manages transition tables, and creates the pg_trigger catalog entry. The function supports all trigger types (BEFORE/AFTER/INSTEAD OF) across different relation kinds (tables, views, foreign tables, partitioned tables) and automatically recurses to create triggers on partitions when appropriate. It also handles trigger replacement with OR REPLACE semantics and establishes proper dependency relationships.

## Parameters / Member Variables
- `stmt`: CreateTrigStmt structure with parsed CREATE TRIGGER statement details
- `queryString`: Source text of CREATE TRIGGER command (needed for WHEN clause parsing)
- `relOid`: Target relation OID (0 to look up by name from stmt->relation)
- `refRelOid`: Constraint reference relation OID (for constraint triggers)
- `constraintOid`: Associated constraint OID (0 for non-constraint triggers)
- `indexOid`: Associated index OID (stored in tgconstrindid field)
- `funcoid`: Trigger function OID (0 to look up from stmt->funcname)
- `parentTriggerOid`: Parent trigger OID for inheritance/partition relationships
- `whenClause`: Pre-transformed WHEN condition (overrides stmt->whenClause)
- `isInternal`: Whether this is an internally-generated trigger
- `in_partition`: Indicates recursive call for partition trigger creation
- `trigger_fires_when`: Firing condition (ORIGIN/ALWAYS/REPLICA/DISABLED)

## Dependencies
- Functions called/Symbols referenced:
  - table_open/table_openrv
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [has_superclass](../h/has_superclass.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [CreateConstraintEntry](CreateConstraintEntry.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)
  - [map_partition_varattnos](../m/map_partition_varattnos.md)
- Called from (representative examples):
  - [CreateTrigger](CreateTrigger.md)
  - [CloneRowTriggersToPartition](CloneRowTriggersToPartition.md)

## Notes and Other Information
- Performs comprehensive relation type validation (tables, views, foreign tables, partitioned tables)
- Handles complex WHEN clause parsing with OLD/NEW variable validation
- Supports transition table validation with extensive restrictions
- Manages trigger name uniqueness for internal triggers by appending OID
- Automatically recurses to partitions for row-level triggers on partitioned tables
- Implements OR REPLACE semantics with proper validation of existing triggers
- Creates proper dependency relationships for functions, constraints, and parent triggers
- Validates trigger function return type must be 'trigger'
- Enforces security with ACL_TRIGGER and ACL_EXECUTE permission checks