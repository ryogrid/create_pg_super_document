# fix_join_expr

## Location
[src/backend/optimizer/plan/setrefs.c:3033-3054](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L3033-L3054)

## Overview
Creates a new set of targetlist entries or join qual clauses by changing varno/varattno values of variables to reference target list values from outer and inner join relations.

## Definition

```c
static List *
fix_join_expr(PlannerInfo *root,
			  List *clauses,
			  indexed_tlist *outer_itlist,
			  indexed_tlist *inner_itlist,
			  Index acceptable_rel,
			  int rtoffset,
			  NullingRelsMatch nrm_match,
			  double num_exec)
```
## Detailed Description
This function transforms variable references in clauses by replacing them with references to target list values from outer and inner join relations. It operates by setting up a context structure and delegating the actual transformation work to fix_join_expr_mutator(). The function also performs opcode lookup and adds regclass OIDs to root->glob->relationOids.

The function supports four different scenarios:
1. **Normal join clause**: All Vars must be replaced by OUTER_VAR or INNER_VAR references
2. **RETURNING clauses**: Replace other-relation Vars with OUTER_VAR references while leaving target Vars alone
3. **ON CONFLICT UPDATE**: Replace EXCLUDED references with INNER_VAR references while leaving target relation Vars alone  
4. **MERGE**: Replace source relation references with INNER_VAR references while leaving target relation Vars alone

## Parameters / Member Variables
- : PlannerInfo structure containing planning context
- : The targetlist or list of join clauses to transform
- : Indexed target list of the outer join relation, or NULL
- : Indexed target list of the inner join relation, or NULL
- : Zero or rangetable index of relation whose Vars may appear without error
- : Amount to increment varnos by
- : Nulling relations match mode (as for search_indexed_tlist_for_var)
- : Estimated number of executions of the expression

## Dependencies
- Functions called/Symbols referenced:
  - [fix_join_expr_mutator](fix_join_expr_mutator.md)
- Data types used:
  - [PlannerInfo](../P/PlannerInfo.md)
  - [List](../L/List.md)
  - [indexed_tlist](../i/indexed_tlist.md)
  - Index
  - NullingRelsMatch
  - [fix_join_expr_context](fix_join_expr_context.md)
  - [Node](../N/Node.md)
- Called from (representative examples):
  - fix_scan_list
  - [set_plan_refs](../s/set_plan_refs.md)
  - [set_join_references](../s/set_join_references.md)
  - [set_returning_clause_references](../s/set_returning_clause_references.md)

## Notes and Other Information
- Returns a new expression tree; the original clause structure is not modified
- The function creates a context structure to pass parameters to the mutator function
- Essential for proper variable reference resolution in different join scenarios
- Part of PostgreSQL's plan tree reference fixing mechanism during query optimization
- The transformation ensures that variable references correctly point to the appropriate target list entries
- Located in src/backend/optimizer/plan/setrefs.c at lines 3033-3054

## Simplified Source

```c
static List *fix_join_expr(PlannerInfo *root,
                          List *clauses,
                          indexed_tlist *outer_itlist,
                          indexed_tlist *inner_itlist,
                          Index acceptable_rel,
                          int rtoffset,
                          NullingRelsMatch nrm_match,
                          double num_exec) {
    // Set up context for variable reference transformation
    fix_join_expr_context context;

    context.root = root;
    context.outer_itlist = outer_itlist;
    context.inner_itlist = inner_itlist;
    context.acceptable_rel = acceptable_rel;
    context.rtoffset = rtoffset;
    context.nrm_match = nrm_match;
    context.num_exec = num_exec;

    // Transform clauses using the mutator function
    return (List *) fix_join_expr_mutator((Node *) clauses, &context);
}
```