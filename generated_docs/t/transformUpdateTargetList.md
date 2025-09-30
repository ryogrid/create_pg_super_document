# transformUpdateTargetList

## Location
[src/backend/parser/analyze.c:2485-2559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L2485-L2559)

## Overview
Handles the SET clause transformation in UPDATE, MERGE, and INSERT...ON CONFLICT UPDATE statements by converting target expressions to proper target list entries with correct column assignments and permission tracking.

## Definition
List *transformUpdateTargetList(ParseState *pstate, List *origTlist)

## Detailed Description
This function transforms the SET clause target list for update operations, ensuring proper column resolution, permission tracking, and target entry structure. The process involves several key steps:

1. Transforms the original target list using standard target list processing with UPDATE_SOURCE expression context
2. Sets up result number assignment for resjunk attributes to avoid conflicts with target table columns
3. Iterates through each transformed target entry to:
   - Handle resjunk entries by assigning non-conflicting result numbers
   - Resolve column names to attribute numbers in the target relation
   - Validate that specified columns exist in the target table
   - Update target list entries with proper column information and indirection handling
   - Track updated columns for permission checking

The function includes comprehensive error handling for undefined columns and provides helpful hints when users incorrectly qualify column names with relation names in SET clauses.

## Parameters / Member Variables
- : Parse state containing target relation information, namespace items, and permission tracking
- : Original target list from the SET clause containing ResTarget nodes

## Dependencies
- Functions called/Symbols referenced:
  - [transformTargetList](transformTargetList.md) (standard target list transformation)
  - EXPR_KIND_UPDATE_SOURCE (expression context for UPDATE sources)
  - RelationGetNumberOfAttributes (gets column count from relation)
  - [list_head](../l/list_head.md) (gets first list cell)
  - [attnameAttNum](../a/attnameAttNum.md) (resolves column name to attribute number)
  - InvalidAttrNumber (invalid attribute constant)
  - [updateTargetListEntry](../u/updateTargetListEntry.md) (updates target entry with column info)
  - [bms_add_member](../b/bms_add_member.md) (adds column to permission bitmap)
  - FirstLowInvalidHeapAttributeNumber (heap attribute numbering base)
  - [lnext](../l/lnext.md) (advances to next list cell)
- Called from (representative examples):
  - [transformUpdateStmt](transformUpdateStmt.md) (UPDATE statement processing)
  - [transformOnConflictClause](transformOnConflictClause.md) (INSERT...ON CONFLICT UPDATE processing)  
  - [transformMergeStmt](transformMergeStmt.md) (MERGE statement processing)

## Notes and Other Information
This function is central to UPDATE operation processing across multiple statement types including standard UPDATE, MERGE, and INSERT...ON CONFLICT UPDATE. It ensures that resjunk entries (system-generated columns) receive result numbers that don't conflict with actual table columns, which is critical for the rewriter and planner. The permission tracking through updatedCols bitmap is essential for PostgreSQL's security model, ensuring proper column-level UPDATE privileges are enforced. The function provides detailed error reporting with location information and helpful hints for common user mistakes like qualifying SET target columns with relation names.

## Simplified Source

```c
List *
transformUpdateTargetList(ParseState *pstate, List *origTlist)
{
    List *tlist = NIL;
    RTEPermissionInfo *target_perminfo;
    ListCell *orig_tl;
    ListCell *tl;

    // Transform the target list using UPDATE_SOURCE context
    tlist = transformTargetList(pstate, origTlist, EXPR_KIND_UPDATE_SOURCE);

    // Prepare to assign non-conflicting resnos to resjunk attributes
    if (pstate->p_next_resno <= RelationGetNumberOfAttributes(pstate->p_target_relation))
        pstate->p_next_resno = RelationGetNumberOfAttributes(pstate->p_target_relation) + 1;

    // Prepare non-junk columns for assignment to target table
    target_perminfo = pstate->p_target_nsitem->p_perminfo;
    orig_tl = list_head(origTlist);

    foreach(tl, tlist) {
        TargetEntry *tle = (TargetEntry *) lfirst(tl);
        ResTarget *origTarget;
        int attrno;

        if (tle->resjunk) {
            // Resjunk nodes need non-conflicting resnos
            tle->resno = (AttrNumber) pstate->p_next_resno++;
            tle->resname = NULL;
            continue;
        }

        if (orig_tl == NULL)
            elog(ERROR, "UPDATE target count mismatch --- internal error");

        origTarget = lfirst_node(ResTarget, orig_tl);

        // Look up column name
        attrno = attnameAttNum(pstate->p_target_relation, origTarget->name, true);
        if (attrno == InvalidAttrNumber) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                 errmsg("column \"%s\" of relation \"%s\" does not exist",
                        origTarget->name,
                        RelationGetRelationName(pstate->p_target_relation)),
                 /* Special hint for qualified column names */
                 (origTarget->indirection != NIL &&
                  strcmp(origTarget->name, pstate->p_target_nsitem->p_names->aliasname) == 0) ?
                 errhint("SET target columns cannot be qualified with the relation name.") : 0,
                 parser_errposition(pstate, origTarget->location)));
        }

        // Update target list entry with column info
        updateTargetListEntry(pstate, tle, origTarget->name,
                              attrno, origTarget->indirection,
                              origTarget->location);

        // Mark column as requiring update permissions
        target_perminfo->updatedCols = bms_add_member(target_perminfo->updatedCols,
                                                      attrno - FirstLowInvalidHeapAttributeNumber);

        orig_tl = lnext(origTlist, orig_tl);
    }

    if (orig_tl != NULL)
        elog(ERROR, "UPDATE target count mismatch --- internal error");

    return tlist;
}
```