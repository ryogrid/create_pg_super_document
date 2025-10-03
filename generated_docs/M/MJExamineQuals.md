# MJExamineQuals

## Location
[src/backend/executor/nodeMergejoin.c:175-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L175-L293)

## Overview
Deconstructs the list of mergejoinable expressions and builds an array of MergeJoinClause structs containing comparison information needed at runtime for merge join execution.

## Definition

```c
static MergeJoinClause
MJExamineQuals(List *mergeclauses,
			   Oid *mergefamilies,
			   Oid *mergecollations,
			   int *mergestrategies,
			   bool *mergenullsfirst,
			   PlanState *parent)
```
## Detailed Description
This function processes the mergejoinable expressions provided by the planner in the form of "leftexpr = rightexpr" expression trees. The expressions are ordered to match the sort columns of the input relations. For each merge clause, the function:

1. Initializes the left and right expressions for execution
2. Sets up sort support data structures with proper collation and ordering
3. Extracts operator family properties to validate equality operators
4. Obtains comparison functions from the operator family, preferring sort support functions over traditional btree comparison functions
5. Creates MergeJoinClause structs containing all necessary runtime comparison information

The function ensures that abbreviation optimization is disabled for merge joins since there's no convenient opportunity to convert to alternative representations during the merge process.

## Parameters / Member Variables
- `*mergeclauses`: List of mergejoinable expression trees from the planner
- `*mergefamilies`: Array of btree operator family OIDs for each merge key
- `*mergecollations`: Array of collation OIDs for each merge key
- `*mergestrategies`: Array of btree strategy numbers (BTLessStrategyNumber or BTGreaterStrategyNumber)
- `*mergenullsfirst`: Array of nulls-first flags indicating null placement in sort order
- `*parent`: Parent plan state node for expression initialization context
## Dependencies
- Functions called/Symbols referenced:
  - [ExecInitExpr](../E/ExecInitExpr.md)
  - [get_op_opfamily_properties](../g/get_op_opfamily_properties.md)
  - [get_opfamily_proc](../g/get_opfamily_proc.md)
  - OidFunctionCall1
  - [PrepareSortSupportComparisonShim](../P/PrepareSortSupportComparisonShim.md)
  - lsecond
  - BTSORTSUPPORT_PROC
  - BTORDER_PROC
- Called from:
  - [ExecInitMergeJoin](../E/ExecInitMergeJoin.md)

## Notes and Other Information
- The function is static and only used internally within the merge join executor
- Validates that all merge clauses are OpExpr nodes and use equality operators
- Prioritizes sort support functions over traditional comparison functions for better performance
- Sets up proper sort ordering (ascending/descending) and null handling based on planner specifications
- Memory allocation uses palloc0 to ensure proper initialization of the MergeJoinClause array

## Simplified Source

```c
static MergeJoinClause
MJExamineQuals(List *mergeclauses,
               Oid *mergefamilies,
               Oid *mergecollations,
               int *mergestrategies,
               bool *mergenullsfirst,
               PlanState *parent)
{
    MergeJoinClause clauses;
    int nClauses = list_length(mergeclauses);
    int iClause = 0;
    ListCell *cl;

    clauses = (MergeJoinClause) palloc0(nClauses * sizeof(MergeJoinClauseData));

    foreach(cl, mergeclauses) {
        OpExpr *qual = (OpExpr *) lfirst(cl);
        MergeJoinClause clause = &clauses[iClause];
        Oid opfamily = mergefamilies[iClause];
        Oid collation = mergecollations[iClause];
        StrategyNumber opstrategy = mergestrategies[iClause];
        bool nulls_first = mergenullsfirst[iClause];

        if (!IsA(qual, OpExpr))
            elog(ERROR, "mergejoin clause is not an OpExpr");

        // Initialize expressions for execution
        clause->lexpr = ExecInitExpr((Expr *) linitial(qual->args), parent);
        clause->rexpr = ExecInitExpr((Expr *) lsecond(qual->args), parent);

        // Set up sort support data
        clause->ssup.ssup_cxt = CurrentMemoryContext;
        clause->ssup.ssup_collation = collation;
        clause->ssup.ssup_nulls_first = nulls_first;

        // Determine sort direction
        if (opstrategy == BTLessStrategyNumber)
            clause->ssup.ssup_reverse = false;
        else if (opstrategy == BTGreaterStrategyNumber)
            clause->ssup.ssup_reverse = true;
        else
            elog(ERROR, "unsupported mergejoin strategy %d", opstrategy);

        // Extract operator properties
        int op_strategy;
        Oid op_lefttype, op_righttype;
        get_op_opfamily_properties(qual->opno, opfamily, false,
                                 &op_strategy, &op_lefttype, &op_righttype);

        if (op_strategy != BTEqualStrategyNumber)
            elog(ERROR, "cannot merge using non-equality operator %u", qual->opno);

        // Abbreviation not applicable for merge joins
        clause->ssup.abbreviate = false;

        // Get comparison function - prefer sort support over traditional btree comparator
        Oid sortfunc = get_opfamily_proc(opfamily, op_lefttype, op_righttype,
                                       BTSORTSUPPORT_PROC);

        if (OidIsValid(sortfunc)) {
            OidFunctionCall1(sortfunc, PointerGetDatum(&clause->ssup));
        }

        if (clause->ssup.comparator == NULL) {
            // Fall back to traditional comparison function
            sortfunc = get_opfamily_proc(opfamily, op_lefttype, op_righttype,
                                       BTORDER_PROC);
            if (!OidIsValid(sortfunc))
                elog(ERROR, "missing support function %d(%u,%u) in opfamily %u",
                     BTORDER_PROC, op_lefttype, op_righttype, opfamily);

            PrepareSortSupportComparisonShim(sortfunc, &clause->ssup);
        }

        iClause++;
    }

    return clauses;
}
```