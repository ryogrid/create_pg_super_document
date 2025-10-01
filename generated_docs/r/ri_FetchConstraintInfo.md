# ri_FetchConstraintInfo

## Location
[src/backend/utils/adt/ri_triggers.c:2058-2111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2058-L2111)

## Overview
Fetches the RI_ConstraintInfo struct for a trigger's foreign key constraint, performing validation checks to ensure the constraint is properly configured.

## Definition

```c
static const RI_ConstraintInfo *
ri_FetchConstraintInfo(Trigger *trigger, Relation trig_rel, bool rel_is_pk)
```
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
  - [ri_LoadConstraintInfo](ri_LoadConstraintInfo.md)
  - OidIsValid
  - ereport
  - RelationGetRelationName
  - RelationGetRelid
  - elog
- Called from (representative examples):
  - [ri_restrict](ri_restrict.md)
  - [RI_FKey_cascade_del](../R/RI_FKey_cascade_del.md)
  - [RI_FKey_cascade_upd](../R/RI_FKey_cascade_upd.md)
  - [ri_set](ri_set.md)
  - [RI_FKey_pk_upd_check_required](../R/RI_FKey_pk_upd_check_required.md)
  - [RI_FKey_fk_upd_check_required](../R/RI_FKey_fk_upd_check_required.md)
  - [RI_Initial_Check](../R/RI_Initial_Check.md)
  - [RI_PartitionRemove_Check](../R/RI_PartitionRemove_Check.md)

## Notes and Other Information
- Returns a const pointer to RI_ConstraintInfo, indicating the returned data should not be modified
- Throws errors for invalid constraint OIDs, mismatched constraint data, or unsupported MATCH PARTIAL constraints
- The function performs different validation logic depending on whether the relation is a primary key table or foreign key table
- Located in src/backend/utils/adt/ri_triggers.c:2058-2111

## Simplified Source

```c
static const RI_ConstraintInfo *ri_FetchConstraintInfo(Trigger *trigger,
                                                      Relation trig_rel,
                                                      bool rel_is_pk) {
    Oid constraintOid = trigger->tgconstraint;

    // Validate constraint OID exists
    if (!OidIsValid(constraintOid))
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                       errmsg("no pg_constraint entry for trigger \"%s\" on table \"%s\"",
                              trigger->tgname, RelationGetRelationName(trig_rel))));

    // Load constraint information from cache/catalog
    const RI_ConstraintInfo *riinfo = ri_LoadConstraintInfo(constraintOid);

    // Validate constraint matches trigger metadata
    if (rel_is_pk) {
        if (riinfo->fk_relid != trigger->tgconstrrelid ||
            riinfo->pk_relid != RelationGetRelid(trig_rel))
            elog(ERROR, "wrong pg_constraint entry for trigger \"%s\" on table \"%s\"",
                 trigger->tgname, RelationGetRelationName(trig_rel));
    } else {
        if (riinfo->fk_relid != RelationGetRelid(trig_rel) ||
            riinfo->pk_relid != trigger->tgconstrrelid)
            elog(ERROR, "wrong pg_constraint entry for trigger \"%s\" on table \"%s\"",
                 trigger->tgname, RelationGetRelationName(trig_rel));
    }

    // Validate match type
    if (riinfo->confmatchtype != FKCONSTR_MATCH_FULL &&
        riinfo->confmatchtype != FKCONSTR_MATCH_PARTIAL &&
        riinfo->confmatchtype != FKCONSTR_MATCH_SIMPLE)
        elog(ERROR, "unrecognized confmatchtype: %d", riinfo->confmatchtype);

    // MATCH PARTIAL not supported
    if (riinfo->confmatchtype == FKCONSTR_MATCH_PARTIAL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("MATCH PARTIAL not yet implemented")));

    return riinfo;
}
```