# checkNameSpaceConflicts

## Location
[src/backend/parser/parse_relation.c:434-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L434-L482)

## Overview
Checks for relation-name conflicts between two namespace lists and raises an error if any conflicts are found.

## Definition
```c
void checkNameSpaceConflicts(ParseState *pstate, List *namespace1, List *namespace2)
```

## Detailed Description
This function validates that two namespace lists can be merged together without creating naming conflicts. It compares each visible relation in the first namespace against all visible relations in the second namespace, checking for duplicate alias names. The function implements SQL standard rules for conflict detection, including the special case where two alias-less plain relation RTEs with the same name do not conflict if they refer to different relation OIDs (indicating they are in different schemas).

The function ignores lateral-only flags when checking conflicts (all items are considered visible for conflict purposes) but does respect the columns-only flag by ignoring items that are not relation-visible.

## Parameters / Member Variables
- `pstate`: Pointer to the ParseState structure (used for error reporting context)
- `namespace1`: First namespace list to check for conflicts
- `namespace2`: Second namespace list to check against the first

## Dependencies
- Functions called/Symbols referenced:
  - ereport/ERROR (for error reporting)
  - strcmp (for name comparison)
  - lfirst (list cell access)
- Types referenced:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [RangeTblEntry](../R/RangeTblEntry.md)
  - RTE_RELATION
- Called from (representative examples):
  - [transformFromClause](../t/transformFromClause.md) (src/backend/parser/parse_clause.c:137)
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (src/backend/parser/parse_clause.c:1215, 1591)

## Notes and Other Information
- The function assumes that each input namespace list does not contain internal conflicts
- Implements SQL standard conflict resolution rules for alias-less relations in different schemas
- Raises ERRCODE_DUPLICATE_ALIAS error when conflicts are detected
- Only checks relation-visible items, ignoring columns-only namespace items
- Lateral-only flags are ignored for conflict detection purposes
- Function is declared in src/include/parser/parse_relation.h
- Used during FROM clause processing to ensure namespace consistency

## Simplified Source

```c
void
checkNameSpaceConflicts(ParseState *pstate, List *namespace1, List *namespace2)
{
    // Check each item in namespace1 against all items in namespace2
    foreach(l1, namespace1) {
        ParseNamespaceItem *nsitem1 = (ParseNamespaceItem *) lfirst(l1);
        RangeTblEntry *rte1 = nsitem1->p_rte;
        const char *aliasname1 = nsitem1->p_names->aliasname;

        // Skip columns-only items (not relation-visible)
        if (!nsitem1->p_rel_visible)
            continue;

        foreach(l2, namespace2) {
            ParseNamespaceItem *nsitem2 = (ParseNamespaceItem *) lfirst(l2);
            RangeTblEntry *rte2 = nsitem2->p_rte;
            const char *aliasname2 = nsitem2->p_names->aliasname;

            // Skip columns-only items
            if (!nsitem2->p_rel_visible)
                continue;

            // No conflict if names are different
            if (strcmp(aliasname2, aliasname1) != 0)
                continue;

            // SQL rule: alias-less plain relations with same name but different OIDs don't conflict
            if (rte1->rtekind == RTE_RELATION && rte1->alias == NULL &&
                rte2->rtekind == RTE_RELATION && rte2->alias == NULL &&
                rte1->relid != rte2->relid)
                continue;

            // Found a conflict - report error
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_ALIAS),
                     errmsg("table name \"%s\" specified more than once", aliasname1)));
        }
    }
}
```