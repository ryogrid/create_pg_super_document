# add_row_identity_var

## Location
[src/backend/optimizer/util/appendinfo.c:789-883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L789-L883)

## Overview
Registers a row-identity column to be used in UPDATE/DELETE/MERGE operations, managing row identification variables for inheritance hierarchies and partitioned tables.

## Definition
void add_row_identity_var(PlannerInfo *root, Var *orig_var, Index rtindex, const char *rowid_name)

## Detailed Description
This function is part of PostgreSQL's row-identity variable management system, which handles row identification across inheritance hierarchies and partitioned tables during UPDATE/DELETE/MERGE operations. When the query planner needs to track specific rows across multiple tables in an inheritance hierarchy, it uses row-identity variables to maintain references to the original rows.

The function creates or updates a RowIdentityVarInfo structure that tracks which relations need a particular row-identity column. For non-inherited operations (where rtindex equals the result relation), it simply adds the variable to the processed target list. For inherited operations, it either finds an existing matching row-identity variable or creates a new one, ensuring that all variables with the same name are structurally equivalent.

The function transforms the original Var into a ROWID_VAR reference and manages a global list of row-identity variables that can be shared across multiple child relations in the inheritance hierarchy.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and row-identity variable tracking
- : The original Var representing the row-identity column (must be a simple Var)
- : Range table index of the relation containing this row-identity column
- : String name for the row-identity column (e.g., "wholerow", "ctid")

## Dependencies
- Functions called/Symbols referenced:
  - [makeTargetEntry](../m/makeTargetEntry.md): Creates target list entries for the processed_tlist
  - copyObject: Creates deep copies of Var nodes
  - [equal](../e/equal.md): Compares row-identity variables for structural equality
  - [bms_is_member](../b/bms_is_member.md): Checks if rtindex is in leaf_result_relids
  - [bms_add_member](../b/bms_add_member.md): Adds rtindex to existing RowIdentityVarInfo
  - [bms_make_singleton](../b/bms_make_singleton.md): Creates single-member bitmapset for new RowIdentityVarInfo
  - [get_typavgwidth](../g/get_typavgwidth.md): Estimates width of the row-identity column
  - [exprType](../e/exprType.md)/exprTypmod: Extract type information from expressions

- Called from (representative examples):
  - [add_row_identity_columns](add_row_identity_columns.md): When adding multiple row-identity columns
  - [expand_single_inheritance_child](../e/expand_single_inheritance_child.md): During inheritance expansion

## Notes and Other Information
- The function enforces that all row-identity variables with the same name must be structurally equivalent (equal() aside from varno)
- For non-inherited UPDATE/DELETE/MERGE operations, the function takes a simpler path and directly adds the variable to processed_tlist
- Row-identity variables use the special varno value ROWID_VAR to distinguish them from regular table variables
- The function maintains a global list in root->row_identity_vars that allows sharing row-identity variables across multiple child relations
- An error is raised if there are conflicting uses of the same row-identity name with different variable structures
- This mechanism is essential for correctly handling UPDATE/DELETE/MERGE operations on partitioned tables and inheritance hierarchies

## Simplified Source

```c
void add_row_identity_var(PlannerInfo *root, Var *orig_var,
                         Index rtindex, const char *rowid_name) {
    // Simple case: non-inherited UPDATE/DELETE/MERGE
    if (rtindex == root->parse->resultRelation) {
        TargetEntry *tle = makeTargetEntry((Expr *) orig_var,
                                          list_length(root->processed_tlist) + 1,
                                          pstrdup(rowid_name), true);
        root->processed_tlist = lappend(root->processed_tlist, tle);
        return;
    }

    // Complex case: inherited operations
    // Convert original var to ROWID_VAR reference
    Var *rowid_var = copyObject(orig_var);
    rowid_var->varno = ROWID_VAR;

    // Look for existing row-identity variable with same name
    foreach(lc, root->row_identity_vars) {
        RowIdentityVarInfo *ridinfo = (RowIdentityVarInfo *) lfirst(lc);
        if (strcmp(rowid_name, ridinfo->rowidname) == 0) {
            if (equal(rowid_var, ridinfo->rowidvar)) {
                // Found match, add this relation to the set
                ridinfo->rowidrels = bms_add_member(ridinfo->rowidrels, rtindex);
                return;
            } else {
                elog(ERROR, "conflicting uses of row-identity name \"%s\"", rowid_name);
            }
        }
    }

    // Create new row-identity variable
    RowIdentityVarInfo *ridinfo = makeNode(RowIdentityVarInfo);
    ridinfo->rowidvar = copyObject(rowid_var);
    ridinfo->rowidwidth = get_typavgwidth(exprType((Node *) rowid_var),
                                         exprTypmod((Node *) rowid_var));
    ridinfo->rowidname = pstrdup(rowid_name);
    ridinfo->rowidrels = bms_make_singleton(rtindex);

    root->row_identity_vars = lappend(root->row_identity_vars, ridinfo);

    // Update rowid_var to reference the new entry
    rowid_var->varattno = list_length(root->row_identity_vars);

    // Add to processed target list
    TargetEntry *tle = makeTargetEntry((Expr *) rowid_var,
                                      list_length(root->processed_tlist) + 1,
                                      pstrdup(rowid_name), true);
    root->processed_tlist = lappend(root->processed_tlist, tle);
}
```