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
- `*riinfo`: Constraint information structure containing constraint details and key mappings
- `pk_rel`: Primary key table relation
- `fk_rel`: Foreign key table relation
- `*violatorslot`: Tuple slot containing the tuple that violated the constraint
- `tupdesc`: Tuple descriptor (can be NULL, will be inferred from relation)
- `queryno`: Query type identifier indicating the kind of RI check that failed
- `partgone`: Boolean indicating if this is a partition removal scenario
## Dependencies
- Functions called/Symbols referenced:
  - [check_enable_rls](../c/check_enable_rls.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [pg_attribute_aclcheck](../p/pg_attribute_aclcheck.md)
  - [slot_getattr](../s/slot_getattr.md)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md)
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md)
  - [errtableconstraint](../e/errtableconstraint.md)
  - RI_PLAN_CHECK_LOOKUPPK (constant)
  - [RLS_ENABLED](../R/RLS_ENABLED.md) (constant)
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

## Simplified Source

```c
static void ri_ReportViolation(const RI_ConstraintInfo *riinfo,
                              Relation pk_rel, Relation fk_rel,
                              TupleTableSlot *violatorslot, TupleDesc tupdesc,
                              int queryno, bool partgone) {
    StringInfoData key_names;
    StringInfoData key_values;
    bool onfk;
    const int16 *attnums;
    Oid rel_oid;
    bool has_perm = true;

    // Determine which relation caused the violation
    onfk = (queryno == RI_PLAN_CHECK_LOOKUPPK);
    if (onfk) {
        attnums = riinfo->fk_attnums;
        rel_oid = fk_rel->rd_id;
        if (tupdesc == NULL)
            tupdesc = fk_rel->rd_att;
    } else {
        attnums = riinfo->pk_attnums;
        rel_oid = pk_rel->rd_id;
        if (tupdesc == NULL)
            tupdesc = pk_rel->rd_att;
    }

    // Check permissions to determine if we can show key details
    if (!partgone) {
        if (check_enable_rls(rel_oid, InvalidOid, true) == RLS_ENABLED) {
            has_perm = false;
        } else {
            AclResult aclresult = pg_class_aclcheck(rel_oid, GetUserId(), ACL_SELECT);
            if (aclresult != ACLCHECK_OK) {
                // Check column-level permissions
                for (int idx = 0; idx < riinfo->nkeys; idx++) {
                    aclresult = pg_attribute_aclcheck(rel_oid, attnums[idx], GetUserId(), ACL_SELECT);
                    if (aclresult != ACLCHECK_OK) {
                        has_perm = false;
                        break;
                    }
                }
            }
        }
    }

    // Build key names and values if permitted
    if (has_perm) {
        initStringInfo(&key_names);
        initStringInfo(&key_values);
        for (int idx = 0; idx < riinfo->nkeys; idx++) {
            int fnum = attnums[idx];
            Form_pg_attribute att = TupleDescAttr(tupdesc, fnum - 1);
            char *name = NameStr(att->attname);

            Datum datum;
            bool isnull;
            datum = slot_getattr(violatorslot, fnum, &isnull);

            char *val;
            if (!isnull) {
                Oid foutoid;
                bool typisvarlena;
                getTypeOutputInfo(att->atttypid, &foutoid, &typisvarlena);
                val = OidOutputFunctionCall(foutoid, datum);
            } else {
                val = "null";
            }

            if (idx > 0) {
                appendStringInfoString(&key_names, ", ");
                appendStringInfoString(&key_values, ", ");
            }
            appendStringInfoString(&key_names, name);
            appendStringInfoString(&key_values, val);
        }
    }

    // Generate appropriate error message based on violation type
    if (partgone) {
        ereport(ERROR,
                (errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
                 errmsg("removing partition \"%s\" violates foreign key constraint \"%s\"",
                        RelationGetRelationName(pk_rel), NameStr(riinfo->conname)),
                 errdetail("Key (%s)=(%s) is still referenced from table \"%s\".",
                          key_names.data, key_values.data, RelationGetRelationName(fk_rel)),
                 errtableconstraint(fk_rel, NameStr(riinfo->conname))));
    } else if (onfk) {
        ereport(ERROR,
                (errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
                 errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\"",
                        RelationGetRelationName(fk_rel), NameStr(riinfo->conname)),
                 has_perm ?
                 errdetail("Key (%s)=(%s) is not present in table \"%s\".",
                          key_names.data, key_values.data, RelationGetRelationName(pk_rel)) :
                 errdetail("Key is not present in table \"%s\".", RelationGetRelationName(pk_rel)),
                 errtableconstraint(fk_rel, NameStr(riinfo->conname))));
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
                 errmsg("update or delete on table \"%s\" violates foreign key constraint \"%s\" on table \"%s\"",
                        RelationGetRelationName(pk_rel), NameStr(riinfo->conname), RelationGetRelationName(fk_rel)),
                 has_perm ?
                 errdetail("Key (%s)=(%s) is still referenced from table \"%s\".",
                          key_names.data, key_values.data, RelationGetRelationName(fk_rel)) :
                 errdetail("Key is still referenced from table \"%s\".", RelationGetRelationName(fk_rel)),
                 errtableconstraint(fk_rel, NameStr(riinfo->conname))));
    }
}
```