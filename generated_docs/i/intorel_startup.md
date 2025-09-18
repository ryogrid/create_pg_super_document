# intorel_startup

## Location
[src/backend/commands/createas.c:452-575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/createas.c#L452-L575)

## Overview
intorel_startup initializes the destination receiver for CREATE TABLE AS and CREATE MATERIALIZED VIEW operations by creating the target relation, setting up column definitions, and preparing the state for bulk tuple insertion.

## Definition
static void intorel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)

## Detailed Description
This function serves as the startup callback for DR_intorel destination receivers. It performs the complete setup process for creating a new relation from query results. The function builds column definitions from the provided tuple descriptor, optionally overriding column names from the IntoClause specification. It then creates the actual target table using create_ctas_internal(), opens it with exclusive access, validates RLS policies, and initializes the state needed for efficient bulk insertion of tuples. For materialized views that will be populated, it tentatively marks them as populated.

## Parameters / Member Variables
- : The DestReceiver object cast to DR_intorel containing the target specification
- : The executor operation type (unused in this function)
- : TupleDesc describing the structure and types of tuples to be inserted

## Dependencies
- Functions called/Symbols referenced:
  - list_head
  - [lnext](../l/lnext.md)
  - makeColumnDef
  - [type_is_collatable](../t/type_is_collatable.md)
  - [create_ctas_internal](../c/create_ctas_internal.md)
  - table_open
  - [check_enable_rls](../c/check_enable_rls.md)
  - [SetMatViewPopulatedState](../S/SetMatViewPopulatedState.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - [GetBulkInsertState](../G/GetBulkInsertState.md)
  - RelationGetTargetBlock
- Called from (representative examples):
  - [CreateIntoRelDestReceiver](../C/CreateIntoRelDestReceiver.md) (sets as callback)
  - DR_intorel structure initialization

## Notes and Other Information
The function supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW by checking the viewQuery field of the IntoClause. It validates that collations can be resolved for collatable types and ensures RLS policies are not enabled (not yet supported for these operations). The function sets up bulk insertion state only when data will actually be inserted (skipData is false). An assertion ensures the target relation's block number is invalid, indicating no prior writes to the relation.