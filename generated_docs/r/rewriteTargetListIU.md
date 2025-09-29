# rewriteTargetListIU

## Location
[src/backend/rewrite/rewriteHandler.c:764-1035](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L764-L1035)

## Overview
Rewrites INSERT/UPDATE target lists into standard form by handling defaults, merging multiple entries for the same attribute, and sorting into canonical order.

## Definition
```c
static List *rewriteTargetListIU(List *targetList, CmdType commandType, OverridingKind override, Relation target_relation, RangeTblEntry *values_rte, int values_rte_index, Bitmapset **unused_values_attrnos)
```

## Detailed Description
rewriteTargetListIU is a comprehensive function that transforms INSERT and UPDATE target lists to ensure proper handling of defaults, constraints, and PostgreSQL-specific features. The function performs three critical responsibilities:

1. **Default Value Processing**: For INSERT operations, adds target list entries to compute default values for any attributes that have defaults but are not explicitly assigned. Replaces explicit DEFAULT specifications with actual column default expressions for both INSERT and UPDATE operations.

2. **Multiple Entry Merging**: Handles cases where the same target attribute appears multiple times, such as partial array or record field updates (e.g., `SET foo[2] = 42, foo[4] = 43`). These are merged into single assignment operations using functions like array_set_element.

3. **Target List Sorting**: Sorts the target list into standard order with non-junk fields ordered by resno, followed by junk fields in arbitrary order.

The function also handles special column types:
- **Identity Columns**: Enforces GENERATED ALWAYS constraints and handles OVERRIDING clauses
- **Generated Columns**: Ensures only DEFAULT values can be inserted/updated into generated columns
- **VALUES RTE Integration**: For multi-row INSERTs using VALUES, tracks which VALUES columns become unused when replaced with defaults

## Parameters / Member Variables
- `targetList`: The original target list to be rewritten
- `commandType`: Command type (CMD_INSERT or CMD_UPDATE)
- `override`: Overriding clause specification (for identity columns)
- `target_relation`: The target relation being modified
- `values_rte`: Range table entry for VALUES clause (NULL if not applicable)
- `values_rte_index`: Index of the VALUES RTE in the range table
- `unused_values_attrnos`: Output parameter for tracking unused VALUES columns

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - [process_matched_tle](../p/process_matched_tle.md)
  - [flatCopyTargetEntry](../f/flatCopyTargetEntry.md)
  - [findDefaultOnlyColumns](../f/findDefaultOnlyColumns.md)
  - [build_column_default](../b/build_column_default.md)
  - [coerce_null_to_domain](../c/coerce_null_to_domain.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [list_concat](../l/list_concat.md)
- Called from (representative examples):
  - [RewriteQuery](../R/RewriteQuery.md) (multiple call sites)

## Notes and Other Information
- This is a static function, only accessible within rewriteHandler.c
- Uses an O(N) algorithm with temporary array to avoid O(N^2) behavior for large attribute counts
- Critical for proper rule rewriting as it must be completed before firing rewrite rules
- Handles complex PostgreSQL features like identity columns (GENERATED ALWAYS/BY DEFAULT)
- Enforces constraints on generated columns that can only accept DEFAULT values
- Optimizes by omitting NULL defaults for INSERT operations (planner handles these)
- For UPDATE operations, explicitly sets NULL values when no default exists
- Properly handles junk attributes (ORDER BY, GROUP BY expressions) by assigning them resnos above real attributes
- Integrates with VALUES RTE processing to track columns that become unused due to default replacement

## Simplified Source

```c
static List *
rewriteTargetListIU(List *targetList, CmdType commandType,
                    OverridingKind override, Relation target_relation,
                    RangeTblEntry *values_rte, int values_rte_index,
                    Bitmapset **unused_values_attrnos)
{
    int numattrs = RelationGetNumberOfAttributes(target_relation);
    TargetEntry **new_tles = palloc0(numattrs * sizeof(TargetEntry *));
    List *new_tlist = NIL;
    List *junk_tlist = NIL;
    int next_junk_attrno = numattrs + 1;
    Bitmapset *default_only_cols = NULL;

    // Process input target list entries
    foreach(temp, targetList) {
        TargetEntry *old_tle = (TargetEntry *) lfirst(temp);

        if (!old_tle->resjunk) {
            // Normal attribute - store in array for processing
            int attrno = old_tle->resno;
            if (attrno < 1 || attrno > numattrs)
                elog(ERROR, "bogus resno %d in targetlist", attrno);

            Form_pg_attribute att_tup = TupleDescAttr(target_relation->rd_att,
                                                      attrno - 1);

            // Skip dropped attributes
            if (att_tup->attisdropped)
                continue;

            // Merge with any prior assignment to same attribute
            new_tles[attrno - 1] = process_matched_tle(old_tle,
                                                       new_tles[attrno - 1],
                                                       NameStr(att_tup->attname));
        } else {
            // Junk attribute - assign new resno and add to junk list
            if (old_tle->resno != next_junk_attrno) {
                old_tle = flatCopyTargetEntry(old_tle);
                old_tle->resno = next_junk_attrno;
            }
            junk_tlist = lappend(junk_tlist, old_tle);
            next_junk_attrno++;
        }
    }

    // Process each attribute in the relation
    for (int attrno = 1; attrno <= numattrs; attrno++) {
        TargetEntry *new_tle = new_tles[attrno - 1];
        Form_pg_attribute att_tup = TupleDescAttr(target_relation->rd_att,
                                                  attrno - 1);

        // Skip dropped attributes
        if (att_tup->attisdropped)
            continue;

        // Determine if we need to apply default value
        bool apply_default = ((new_tle == NULL && commandType == CMD_INSERT) ||
                             (new_tle && new_tle->expr &&
                              IsA(new_tle->expr, SetToDefault)));

        if (commandType == CMD_INSERT) {
            // Handle identity column constraints
            if (att_tup->attidentity == ATTRIBUTE_IDENTITY_ALWAYS &&
                !apply_default) {
                if (override == OVERRIDING_USER_VALUE)
                    apply_default = true;
                else if (override != OVERRIDING_SYSTEM_VALUE) {
                    // Check if VALUES column contains only DEFAULT
                    int values_attrno = get_values_attrno(new_tle, values_rte_index);
                    if (values_attrno && is_default_only_column(values_attrno,
                                                              values_rte,
                                                              &default_only_cols))
                        apply_default = true;

                    if (!apply_default)
                        ereport(ERROR, "cannot insert non-DEFAULT into GENERATED ALWAYS");
                }
            }

            // Handle generated columns
            if (att_tup->attgenerated && !apply_default) {
                int values_attrno = get_values_attrno(new_tle, values_rte_index);
                if (values_attrno && is_default_only_column(values_attrno,
                                                          values_rte,
                                                          &default_only_cols))
                    apply_default = true;

                if (!apply_default)
                    ereport(ERROR, "cannot insert non-DEFAULT into generated column");
            }

            // Track unused VALUES columns
            if (values_attrno && apply_default && unused_values_attrnos)
                *unused_values_attrnos = bms_add_member(*unused_values_attrnos,
                                                        values_attrno);
        }

        if (commandType == CMD_UPDATE) {
            // UPDATE constraints for identity and generated columns
            if (att_tup->attidentity == ATTRIBUTE_IDENTITY_ALWAYS &&
                new_tle && !apply_default)
                ereport(ERROR, "identity column can only be updated to DEFAULT");

            if (att_tup->attgenerated && new_tle && !apply_default)
                ereport(ERROR, "generated column can only be updated to DEFAULT");
        }

        // Handle different column types
        if (att_tup->attgenerated) {
            // Generated columns handled in executor
            new_tle = NULL;
        } else if (apply_default) {
            // Build default expression
            Node *new_expr = build_column_default(target_relation, attrno);

            if (!new_expr) {
                if (commandType == CMD_INSERT)
                    new_tle = NULL;  // Let planner insert NULL
                else
                    new_expr = coerce_null_to_domain(att_tup->atttypid,
                                                     att_tup->atttypmod,
                                                     att_tup->attcollation,
                                                     att_tup->attlen,
                                                     att_tup->attbyval);
            }

            if (new_expr)
                new_tle = makeTargetEntry((Expr *) new_expr, attrno,
                                          pstrdup(NameStr(att_tup->attname)),
                                          false);
        }

        if (new_tle)
            new_tlist = lappend(new_tlist, new_tle);
    }

    pfree(new_tles);
    return list_concat(new_tlist, junk_tlist);
}
```