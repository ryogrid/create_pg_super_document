# ri_ReportViolation

## Location
[src/backend/utils/adt/ri_triggers.c:2478-2635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2478-L2635)

## Overview
Generates and reports detailed error messages for referential integrity constraint violations, with appropriate permission checking and data formatting.

## Definition

```c
static void
ri_ReportViolation(const RI_ConstraintInfo *riinfo,
				   Relation pk_rel, Relation fk_rel,
				   TupleTableSlot *violatorslot, TupleDesc tupdesc,
				   int queryno, bool partgone)
```
## Detailed Description
This function produces comprehensive error reports when foreign key constraints are violated. It determines the appropriate error message format based on the type of violation (insert/update on FK table vs update/delete on PK table), extracts and formats the violating key values for display, and performs extensive permission checking to ensure users only see data they have access to. The function handles special cases like partition removal and respects Row Level Security (RLS) policies when deciding whether to include detailed key information in error messages.

The function creates user-friendly error messages that include constraint names, table names, and when permitted, the actual key values that caused the violation.

## Parameters / Member Variables
- : Constraint information structure containing constraint details and key mappings
- : Primary key table relation
- : Foreign key table relation
- : Tuple slot containing the tuple that violated the constraint
- : Tuple descriptor (can be NULL, will be inferred from relation)
- : Query type identifier indicating the kind of RI check that failed
- : Boolean indicating if this is a partition removal scenario

## Dependencies
- Functions called/Symbols referenced:
  - [check_enable_rls](../c/check_enable_rls.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [pg_attribute_aclcheck](../p/pg_attribute_aclcheck.md)
  - slot_getattr
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md)
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md)
  - [errtableconstraint](../e/errtableconstraint.md)
  - RI_PLAN_CHECK_LOOKUPPK (constant)
  - RLS_ENABLED (constant)
  - ACL_SELECT (constant)
- Called from (representative examples):
  - [RI_Initial_Check](../R/RI_Initial_Check.md)
  - [RI_PartitionRemove_Check](../R/RI_PartitionRemove_Check.md)
  - [ri_PerformCheck](ri_PerformCheck.md)

## Notes and Other Information
- Performs comprehensive permission checking before revealing key values in error messages
- Respects Row Level Security (RLS) settings to prevent information leakage
- Formats key values using appropriate output functions for each data type
- Provides different error message formats depending on violation type (FK insert/update vs PK update/delete)
- Handles special case of partition removal with dedicated error messaging
- Falls back to generic error messages when user lacks permission to see detailed key information
- Uses StringInfo for efficient string building when constructing key names and values