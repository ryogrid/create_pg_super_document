# ATExecValidateConstraint

## Location
[src/backend/commands/tablecmds.c:11704-11892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L11704-L11892)

## Overview
ATExecValidateConstraint implements the ALTER TABLE VALIDATE CONSTRAINT command, which validates a previously created NOT VALID constraint by checking all existing data against the constraint and marking it as validated in the catalog.

## Definition

```c
static ObjectAddress
ATExecValidateConstraint(List **wqueue, Relation rel, char *constrName,
						 bool recurse, bool recursing, LOCKMODE lockmode)
```
## Detailed Description
This function validates foreign key and check constraints that were previously created with the NOT VALID option. It finds the target constraint, verifies it's an appropriate type (foreign key or check), and if not already validated, queues the validation work for phase 3 of ALTER TABLE processing. The function handles both foreign key and check constraints differently:

For foreign key constraints:
- Creates a NewConstraint entry and queues it for validation
- Does not handle recursion since invalid foreign keys on partitioned tables are disallowed

For check constraints:
- Recursively validates child table constraints first to avoid deadlocks
- Requires all child constraints to be validated before parent validation
- Extracts the constraint expression and queues validation work

The function updates the constraint catalog entry to mark it as validated only after queueing the validation work.

## Parameters / Member Variables
- : Work queue for ALTER TABLE operations to add validation tasks
- : The relation containing the constraint to validate
- : Name of the constraint to validate
- : Whether to recursively validate constraints on child tables
- : Whether this call is part of a recursive operation
- : Lock mode to use when accessing child relations

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - makeNode
  - [palloc0](../p/palloc0.md)
  - [ATGetQueueEntry](ATGetQueueEntry.md)
  - [lappend](../l/lappend.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [ATExecValidateConstraint](ATExecValidateConstraint.md) (recursive self-call)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - TextDatumGetCString
  - [stringToNode](../s/stringToNode.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)
  - [ATExecValidateConstraint](ATExecValidateConstraint.md) (recursive calls for child table constraints)

## Notes and Other Information
- Only works with foreign key and check constraints; other constraint types result in an error
- Validation is queued for phase 3 processing rather than performed immediately
- For check constraints with inheritance, all child constraints must be validated first
- Returns InvalidObjectAddress if the constraint was already validated
- The actual constraint checking is deferred to phase 3 to avoid holding locks too long
- Handles recursion at this level rather than phase 1 to optimize locking for foreign keys