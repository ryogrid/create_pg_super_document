# ri_FetchConstraintInfo

## Location
src/backend/utils/adt/ri_triggers.c: 2058 - 2111

## Overview
Fetches the RI_ConstraintInfo struct for a trigger's foreign key constraint, performing validation checks to ensure the constraint is properly configured.

## Definition


## Detailed Description
This function retrieves constraint information for referential integrity triggers by looking up the foreign key constraint associated with a trigger. It performs several validation steps:

1. Validates that the trigger has a valid constraint OID
2. Loads the constraint information using ri_LoadConstraintInfo
3. Cross-checks the constraint data against trigger metadata
4. Validates the constraint match type and ensures MATCH PARTIAL is not used (unsupported)

The function is critical for ensuring that referential integrity triggers operate on valid, properly configured foreign key constraints.

## Parameters / Member Variables
- : Pointer to the Trigger struct containing trigger metadata including constraint OID
- : Relation on which the trigger is defined
- : Boolean indicating whether trig_rel is the primary key (referenced) table

## Dependencies
- Functions called/Symbols referenced:
  - ri_LoadConstraintInfo
  - OidIsValid
  - ereport
  - RelationGetRelationName
  - RelationGetRelid
  - elog
- Called from (representative examples):
  - ri_restrict
  - RI_FKey_cascade_del
  - RI_FKey_cascade_upd
  - ri_set
  - RI_FKey_pk_upd_check_required
  - RI_FKey_fk_upd_check_required
  - RI_Initial_Check
  - RI_PartitionRemove_Check

## Notes and Other Information
- Returns a const pointer to RI_ConstraintInfo, indicating the returned data should not be modified
- Throws errors for invalid constraint OIDs, mismatched constraint data, or unsupported MATCH PARTIAL constraints
- The function performs different validation logic depending on whether the relation is a primary key table or foreign key table
- Located in src/backend/utils/adt/ri_triggers.c:2058-2111