# CreateTrigger

## Location
[src/backend/commands/trigger.c:159-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L159-L175)

## Overview
CreateTrigger is a PostgreSQL function that creates a new trigger on a specified relation, serving as a wrapper around CreateTriggerFiringOn with default firing behavior.

## Definition

```c
ObjectAddress
CreateTrigger(CreateTrigStmt *stmt, const char *queryString,
			  Oid relOid, Oid refRelOid, Oid constraintOid, Oid indexOid,
			  Oid funcoid, Oid parentTriggerOid, Node *whenClause,
			  bool isInternal, bool in_partition)
```
## Detailed Description
CreateTrigger is a simplified interface for creating triggers that delegates to CreateTriggerFiringOn with the default TRIGGER_FIRES_ON_ORIGIN firing behavior. This function handles the creation of database triggers with support for constraint triggers, partitioned tables, and various internal trigger scenarios. It provides extensive parameter flexibility to support different trigger creation contexts including user-initiated CREATE TRIGGER commands and internally generated triggers for constraints and foreign keys.

## Parameters / Member Variables
- `*stmt`: CreateTrigStmt structure containing the parsed CREATE TRIGGER statement
- `*queryString`: Source text of the CREATE TRIGGER command (required if whenClause is specified)
- `relOid`: OID of the relation on which to create the trigger (0 to look up by name)
- `refRelOid`: OID of the constraint reference relation (0 to look up by name)
- `constraintOid`: OID of the constraint this trigger implements (0 for user triggers)
- `indexOid`: OID of associated constraint index (stored in pg_trigger.tgconstrindid)
- `funcoid`: OID of the trigger function (0 to use stmt->funcname)
- `parentTriggerOid`: OID of parent trigger for inheritance relationships
- `*whenClause`: Pre-transformed WHEN expression (overrides stmt->whenClause)
- `isInternal`: Whether this is an internally-generated trigger
- `in_partition`: Indicates recursive call for partition triggers
## Dependencies
- Functions called/Symbols referenced:
  - [CreateTriggerFiringOn](CreateTriggerFiringOn.md)
  - TRIGGER_FIRES_ON_ORIGIN
  - [CreateTrigStmt](CreateTrigStmt.md)
- Called from (representative examples):
  - [index_constraint_create](../i/index_constraint_create.md)
  - [CreateFKCheckTrigger](CreateFKCheckTrigger.md)
  - [createForeignKeyActionTriggers](../c/createForeignKeyActionTriggers.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- This function is essentially a wrapper that provides backward compatibility and simplified interface
- Automatically handles recursion to partitioned tables when isInternal is false
- Requires ACL_TRIGGER permissions on the relation and ACL_EXECUTE on trigger function for non-internal triggers
- Internal triggers bypass permission checks but caller must handle them appropriately
- Returns ObjectAddress of the created trigger for dependency tracking

## Simplified Source

```c
ObjectAddress CreateTrigger(CreateTrigStmt *stmt, const char *queryString,
                           Oid relOid, Oid refRelOid, Oid constraintOid, Oid indexOid,
                           Oid funcoid, Oid parentTriggerOid, Node *whenClause,
                           bool isInternal, bool in_partition) {
    // Delegate to CreateTriggerFiringOn with default firing behavior
    return CreateTriggerFiringOn(stmt, queryString, relOid, refRelOid,
                                constraintOid, indexOid, funcoid,
                                parentTriggerOid, whenClause, isInternal,
                                in_partition, TRIGGER_FIRES_ON_ORIGIN);
}
```