# set_using_names

## Location
[src/backend/utils/adt/ruleutils.c:4145-4309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L4145-L4309)

## Overview
Recursively assigns column aliases for merged USING columns during rule decompilation by traversing the query jointree and selecting appropriate unique column names.

## Definition

```c
structs */
		leftcolinfo = deparse_columns_fetch(colinfo->leftrti, dpns);
```
## Detailed Description
This function performs a recursive descent of the query jointree to select column aliases that will be used for merged USING columns in JOIN operations. It implements two strategies for name uniqueness depending on the dpns->unique_using flag:

1. **Global uniqueness strategy** (dpns->unique_using = true): Forces all USING names to be unique across the entire query level
2. **Local uniqueness strategy** (dpns->unique_using = false): Only requires USING names to be unique within their own join RTE

The function handles different types of jointree nodes:
- **RangeTblRef**: No action required
- **FromExpr**: Recursively processes all items in the fromlist
- **JoinExpr**: Main processing logic for join operations, including USING clause handling

For joins with USING clauses, the function selects appropriate column names by preferring user-written output aliases, honoring pushed-down names from parent joins, and ensuring uniqueness as required. Selected names are propagated down to child relations and stored for later use during rule decompilation.

## Parameters / Member Variables
- : Deparse namespace context containing column name tracking information and uniqueness strategy
- : Current jointree node being processed (RangeTblRef, FromExpr, or JoinExpr)
- : List of all USING aliases assigned in parent joins (must not be modified)

## Dependencies
- Functions called/Symbols referenced:
  - [identify_join_columns](../i/identify_join_columns.md)
  - rt_fetch
  - deparse_columns_fetch
  - [expand_colnames_array_to](../e/expand_colnames_array_to.md)
  - [make_colname_unique](../m/make_colname_unique.md)
  - [list_copy](../l/list_copy.md)
  - [list_nth](../l/list_nth.md)
  - nodeTag
- Called from (representative examples):
  - [set_deparse_for_query](set_deparse_for_query.md)
  - [set_using_names](set_using_names.md) (recursive calls)

## Notes and Other Information
- This function is part of PostgreSQL's rule decompilation system in ruleutils.c
- The function assumes that dpns->rtable_columns is already filled with pre-zeroed deparse_columns structs
- For unnamed joins, name requirements are pushed down to children rather than being resolved at the current level
- The function maintains parent-child relationships in the deparse_columns structures for proper name inheritance
- System columns (attribute numbers ≤ 0) are handled specially and not assigned names

## Simplified Source
```c
static void set_using_names(deparse_namespace *dpns, Node *jtnode, List *parentUsing) {
    if (IsA(jtnode, RangeTblRef)) {
        // Base case: nothing to do for simple table references
        return;
    }
    else if (IsA(jtnode, FromExpr)) {
        // Recursively process all items in FROM list
        FromExpr *f = (FromExpr *) jtnode;
        ListCell *lc;

        foreach(lc, f->fromlist)
            set_using_names(dpns, (Node *) lfirst(lc), parentUsing);
    }
    else if (IsA(jtnode, JoinExpr)) {
        JoinExpr *j = (JoinExpr *) jtnode;
        RangeTblEntry *rte = rt_fetch(j->rtindex, dpns->rtable);
        deparse_columns *colinfo = deparse_columns_fetch(j->rtindex, dpns);

        // Get join shape information
        identify_join_columns(j, rte, colinfo);
        int *leftattnos = colinfo->leftattnos;
        int *rightattnos = colinfo->rightattnos;

        // Get child column info structures
        deparse_columns *leftcolinfo = deparse_columns_fetch(colinfo->leftrti, dpns);
        deparse_columns *rightcolinfo = deparse_columns_fetch(colinfo->rightrti, dpns);

        // For unnamed joins, push down any required names to children
        if (rte->alias == NULL) {
            for (int i = 0; i < colinfo->num_cols; i++) {
                char *colname = colinfo->colnames[i];
                if (colname == NULL)
                    continue;

                // Push down to left child (if not system column)
                if (leftattnos[i] > 0) {
                    expand_colnames_array_to(leftcolinfo, leftattnos[i]);
                    leftcolinfo->colnames[leftattnos[i] - 1] = colname;
                }

                // Push down to right child (if not system column)
                if (rightattnos[i] > 0) {
                    expand_colnames_array_to(rightcolinfo, rightattnos[i]);
                    rightcolinfo->colnames[rightattnos[i] - 1] = colname;
                }
            }
        }

        // Handle USING clause column name selection
        if (j->usingClause) {
            parentUsing = list_copy(parentUsing);  // Don't modify input list

            expand_colnames_array_to(colinfo, list_length(j->usingClause));
            int i = 0;
            ListCell *lc;

            foreach(lc, j->usingClause) {
                char *colname = strVal(lfirst(lc));

                // Use pushed-down name if available
                if (colinfo->colnames[i] != NULL) {
                    colname = colinfo->colnames[i];
                } else {
                    // Prefer user-written alias if available
                    if (rte->alias && i < list_length(rte->alias->colnames))
                        colname = strVal(list_nth(rte->alias->colnames, i));

                    // Make unique according to strategy
                    colname = make_colname_unique(colname, dpns, colinfo);

                    // For global strategy, track all USING names
                    if (dpns->unique_using)
                        dpns->using_names = lappend(dpns->using_names, colname);

                    colinfo->colnames[i] = colname;
                }

                // Remember for later use and add to parent list
                colinfo->usingNames = lappend(colinfo->usingNames, colname);
                parentUsing = lappend(parentUsing, colname);

                // Push down to child columns
                if (leftattnos[i] > 0) {
                    expand_colnames_array_to(leftcolinfo, leftattnos[i]);
                    leftcolinfo->colnames[leftattnos[i] - 1] = colname;
                }
                if (rightattnos[i] > 0) {
                    expand_colnames_array_to(rightcolinfo, rightattnos[i]);
                    rightcolinfo->colnames[rightattnos[i] - 1] = colname;
                }

                i++;
            }
        }

        // Set parent context and recurse
        leftcolinfo->parentUsing = parentUsing;
        rightcolinfo->parentUsing = parentUsing;

        set_using_names(dpns, j->larg, parentUsing);
        set_using_names(dpns, j->rarg, parentUsing);
    }
}
```