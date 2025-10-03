# fix_indexqual_clause

## Location
[src/backend/optimizer/plan/createplan.c:5093-5163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5093-L5163)

## Overview
Converts a single indexqual clause to the form needed by PostgreSQL's executor, handling parameter replacement and index key variable transformation.

## Definition

```c
static Node *
fix_indexqual_clause(PlannerInfo *root, IndexOptInfo *index, int indexcol,
					 Node *clause, List *indexcolnos)
```
## Detailed Description
This function performs the core transformation logic for individual index qualification clauses, preparing them for execution. It operates in two main phases:

1. **Parameter Replacement**: Uses replace_nestloop_params() to replace any outer-relation variables with nestloop parameters, which also creates a safe copy of the clause for in-place modification.

2. **Index Key Transformation**: Replaces index key variables or expressions with proper index Var nodes that reference the index's attribute numbers rather than the original relation's attribute numbers.

The function handles multiple types of index qualification clauses:
- **OpExpr**: Standard operator expressions (e.g., column = value)
- **RowCompareExpr**: Row comparison expressions for multi-column indexes
- **ScalarArrayOpExpr**: Array comparison expressions (e.g., column = ANY(array))
- **NullTest**: NULL/NOT NULL tests on index columns

For each clause type, it identifies the index key operand(s) and calls fix_indexqual_operand() to perform the actual variable replacement. Row comparisons require special handling to process multiple index columns simultaneously.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner context and state
- `*index`: IndexOptInfo describing the index being used
- `indexcol`: Index column number being referenced (for single-column cases)
- `*clause`: The qualification clause to be transformed
- `*indexcolnos`: List of index column numbers (used for multi-column row comparisons)
## Dependencies
- Functions called/Symbols referenced:
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [fix_indexqual_operand](fix_indexqual_operand.md)
  - forboth (macro)
  - lfirst_int
  - nodeTag
  - [IndexOptInfo](../I/IndexOptInfo.md) (struct type)
  - [OpExpr](../O/OpExpr.md) (struct type)
  - RowCompareExpr (struct type)
  - [ScalarArrayOpExpr](../S/ScalarArrayOpExpr.md) (struct type)  
  - [NullTest](../N/NullTest.md) (struct type)
- Called from (representative examples):
  - [fix_indexqual_references](fix_indexqual_references.md)
  - [fix_indexorderby_references](fix_indexorderby_references.md)

## Notes and Other Information
This function is a critical component in the index scan execution preparation process. It ensures that index qualifications are properly parameterized and use the correct attribute references for the target index. The function creates a copy of the input clause during parameter replacement, making it safe for in-place modifications. The comprehensive handling of different clause types reflects the variety of ways indexes can be used in PostgreSQL queries. Error handling ensures that unsupported qualification types are caught during planning rather than execution. Located in src/backend/optimizer/plan/createplan.c at lines 5093-5163.

## Simplified Source

```c
static Node *
fix_indexqual_clause(PlannerInfo *root, IndexOptInfo *index, int indexcol,
                     Node *clause, List *indexcolnos)
{
    // Replace outer-relation variables with nestloop params (also copies clause)
    clause = replace_nestloop_params(root, clause);

    if (IsA(clause, OpExpr)) {
        // Standard operator expression: column = value
        OpExpr *op = (OpExpr *) clause;
        linitial(op->args) = fix_indexqual_operand(linitial(op->args), index, indexcol);
    }
    else if (IsA(clause, RowCompareExpr)) {
        // Multi-column row comparison
        RowCompareExpr *rc = (RowCompareExpr *) clause;
        ListCell *lca, *lcai;

        // Fix each index key expression
        forboth(lca, rc->largs, lcai, indexcolnos) {
            lfirst(lca) = fix_indexqual_operand(lfirst(lca), index, lfirst_int(lcai));
        }
    }
    else if (IsA(clause, ScalarArrayOpExpr)) {
        // Array operation: column = ANY(array)
        ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
        linitial(saop->args) = fix_indexqual_operand(linitial(saop->args), index, indexcol);
    }
    else if (IsA(clause, NullTest)) {
        // NULL/NOT NULL test
        NullTest *nt = (NullTest *) clause;
        nt->arg = (Expr *) fix_indexqual_operand((Node *) nt->arg, index, indexcol);
    }
    else {
        elog(ERROR, "unsupported indexqual type: %d", (int) nodeTag(clause));
    }

    return clause;
}
```