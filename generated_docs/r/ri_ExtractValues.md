# ri_ExtractValues

## Location
[src/backend/utils/adt/ri_triggers.c:2449-2477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2449-L2477)

## Overview
Extracts field values from a tuple slot into Datum and nulls arrays for use in referential integrity constraint queries.

## Definition

```c
static void
ri_ExtractValues(Relation rel, TupleTableSlot *slot,
				 const RI_ConstraintInfo *riinfo, bool rel_is_pk,
				 Datum *vals, char *nulls)
```
## Detailed Description
This utility function extracts attribute values from a tuple slot and stores them in arrays suitable for passing to SPI queries. It selects the appropriate attribute numbers based on whether the relation is the primary key or foreign key table, then extracts each value using slot_getattr and converts null indicators to the character format expected by SPI functions ('n' for null, ' ' for not null).

The function is essential for preparing parameter values when executing referential integrity queries, ensuring that the correct columns are extracted based on the constraint definition.

## Parameters / Member Variables
- `rel`: The relation (table) from which values are being extracted
- `*slot`: Tuple slot containing the tuple data to extract from
- `*riinfo`: Constraint information structure defining which attributes to extract
- `rel_is_pk`: Boolean indicating whether this relation is the primary key table (true) or foreign key table (false)
- `*vals`: Output array to store the extracted Datum values
- `*nulls`: Output array to store null indicators as characters
## Dependencies
- Functions called/Symbols referenced:
  - [slot_getattr](../s/slot_getattr.md)
  - [RI_ConstraintInfo](../R/RI_ConstraintInfo.md) (structure access)
- Called from (representative examples):
  - [ri_PerformCheck](ri_PerformCheck.md) (called multiple times to extract old and new tuple values)

## Notes and Other Information
- Uses different attribute number arrays (pk_attnums vs fk_attnums) based on the rel_is_pk parameter
- Converts boolean null indicators to character format required by SPI ('n' for null, ' ' for non-null)
- Extracts exactly riinfo->nkeys values, corresponding to the number of columns in the foreign key constraint
- Essential building block for all referential integrity query execution