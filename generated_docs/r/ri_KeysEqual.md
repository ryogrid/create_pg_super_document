# ri_KeysEqual

## Location
[src/backend/utils/adt/ri_triggers.c:2795-2865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2795-L2865)

## Overview
Compares key values between old and new tuple slots to determine if they are equal, using different comparison strategies for primary key and foreign key attributes.

## Definition
```c
static bool ri_KeysEqual(Relation rel, TupleTableSlot *oldslot, TupleTableSlot *newslot, const RI_ConstraintInfo *riinfo, bool rel_is_pk)
```

## Detailed Description
This function performs key comparison between old and new tuple values in the context of referential integrity constraint checking. It uses different comparison strategies depending on whether the relation is a primary key or foreign key table:

- For primary key tables: Uses bytewise comparison via datum_image_eq to detect any physical changes, even if they would be considered equal by the equality operator. This ensures proper cascade behavior for ON UPDATE CASCADE.
- For foreign key tables: Uses semantic equality comparison via the appropriate equality operators, as changes that compare equal will still satisfy the constraint.

The function returns false immediately if any key attribute is NULL in either tuple, or if any corresponding key values are not equal according to the appropriate comparison method.

## Parameters / Member Variables
- `rel`: The relation being compared (either PK or FK table)
- `oldslot`: Tuple slot containing the old (original) key values
- `newslot`: Tuple slot containing the new (updated) key values  
- `riinfo`: Referential integrity constraint information containing key attributes and operators
- `rel_is_pk`: Boolean flag indicating whether this relation is the primary key table (true) or foreign key table (false)

## Dependencies
- Functions called/Symbols referenced:
  - slot_getattr (retrieves attribute values from tuple slots)
  - [datum_image_eq](../d/datum_image_eq.md) (performs bytewise comparison for PK attributes)
  - [ri_AttributesEqual](ri_AttributesEqual.md) (performs semantic equality comparison for FK attributes)
  - RIAttType (gets attribute type information)
- Called from (representative examples):
  - [RI_FKey_pk_upd_check_required](../R/RI_FKey_pk_upd_check_required.md) (checks if PK update requires FK constraint checking)
  - [RI_FKey_fk_upd_check_required](../R/RI_FKey_fk_upd_check_required.md) (checks if FK update requires constraint validation)

## Notes and Other Information
- Returns false if any key attribute contains NULL values in either tuple
- Uses different comparison semantics for PK vs FK tables to handle update propagation correctly
- Could potentially be enhanced to support "IS NOT DISTINCT" semantics to treat NULLs as equal
- Performance optimization opportunity exists to fetch all required attributes at once rather than individually
- Critical component for determining when referential integrity constraint checks are needed during updates