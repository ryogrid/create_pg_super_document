# pullup_replace_vars_context

## Location
[src/backend/optimizer/prep/prepjointree.c:56-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L56-L69)

## Overview
The pullup_replace_vars_context struct serves as a comprehensive context structure for variable replacement operations during subquery pullup optimization, containing all necessary information to correctly transform variable references when flattening subqueries into their parent queries.

## Definition
```c
typedef struct pullup_replace_vars_context
{
    PlannerInfo *root;
    List       *targetlist;        /* tlist of subquery being pulled up */
    RangeTblEntry *target_rte;     /* RTE of subquery */
    Relids      relids;            /* relids within subquery, as numbered after
                                    * pullup (set only if target_rte->lateral) */
    nullingrel_info *nullinfo;    /* per-RTE nullingrel info (set only if
                                    * target_rte->lateral) */
    bool       *outer_hasSubLinks; /* -> outer query's hasSubLinks */
    int         varno;             /* varno of subquery */
    bool        wrap_non_vars;     /* do we need all non-Var outputs to be PHVs? */
    Node      **rv_cache;          /* cache for results with PHVs */
} pullup_replace_vars_context;
```

## Detailed Description
This context structure is essential for PostgreSQL's subquery pullup optimization, which attempts to flatten subqueries into their parent queries to enable better optimization opportunities. The structure contains all the information needed to correctly replace variable references when transforming a subquery's expressions to work in the context of the parent query.

The pullup process involves complex variable renumbering and reference adjustments, especially when dealing with lateral subqueries that can reference variables from outer query levels. This context ensures that all variable replacements maintain semantic correctness while enabling the optimizer to work with a flatter query structure.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo structure containing global planning information
- `targetlist`: Target list of the subquery being pulled up, used for variable replacement mapping
- `target_rte`: Range table entry of the subquery being pulled up
- `relids`: Set of relation IDs within the subquery, renumbered after pullup; only set for lateral subqueries
- `nullinfo`: Per-RTE nulling relation information, used only for lateral subqueries to handle outer join semantics correctly
- `outer_hasSubLinks`: Pointer to the outer query's hasSubLinks flag, updated during pullup if sublinks are encountered
- `varno`: Variable number (range table index) of the subquery being pulled up
- `wrap_non_vars`: Boolean flag indicating whether non-Var expressions need to be wrapped as PlaceHolderVars (PHVs)
- `rv_cache`: Cache array for storing results with PlaceHolderVars to avoid redundant computations

## Dependencies
- Functions called/Symbols referenced:
  - [nullingrel_info](../n/nullingrel_info.md)
  - [PlannerInfo](../P/PlannerInfo.md)
  - [List](../L/List.md)
  - [RangeTblEntry](../R/RangeTblEntry.md)
  - Relids
  - [Node](../N/Node.md)
- Called from (representative examples):
  - [pull_up_simple_subquery](pull_up_simple_subquery.md)
  - [pull_up_simple_values](pull_up_simple_values.md)
  - [pull_up_constant_function](pull_up_constant_function.md)
  - [perform_pullup_replace_vars](perform_pullup_replace_vars.md)
  - [replace_vars_in_jointree](../r/replace_vars_in_jointree.md)
  - [pullup_replace_vars](pullup_replace_vars.md)
  - [pullup_replace_vars_callback](pullup_replace_vars_callback.md)
  - [pullup_replace_vars_subquery](pullup_replace_vars_subquery.md)

## Notes and Other Information
This structure is particularly important for handling lateral subqueries, which can reference outer variables and require special handling of nulling relationships. The rv_cache mechanism helps optimize performance by avoiding repeated computation of the same PlaceHolderVar expressions. The wrap_non_vars flag is crucial for maintaining expression evaluation semantics in the pulled-up query.