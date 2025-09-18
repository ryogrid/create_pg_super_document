# replace_rte_variables_context

## Location
src/include/rewrite/rewriteManip.h: 22 - 27

## Overview
A context structure that carries state and configuration information for the variable replacement callback mechanism used during query tree transformations.

## Definition
```c
struct replace_rte_variables_context
{
    replace_rte_variables_callback callback;    /* callback function */
    void       *callback_arg;   /* context data for callback function */
    int         target_varno;   /* RTE index to search for */
    int         sublevels_up;   /* (current) nesting depth */
    bool        inserted_sublink;   /* have we inserted a SubLink? */
};
```

## Detailed Description
The `replace_rte_variables_context` structure serves as a parameter passing mechanism for the `replace_rte_variables_mutator` function and related query rewriting operations. It encapsulates all the necessary state information needed to perform variable replacement operations during query tree traversal. This structure is typically used in conjunction with a callback function that defines the specific replacement logic for variables (Var nodes) found in the query tree.

The structure supports a flexible callback-based architecture where different replacement strategies can be implemented by providing different callback functions, while the traversal logic remains consistent. The context tracks important state such as the target relation (varno), nesting level, and whether any SubLinks have been inserted during the transformation process.

## Parameters / Member Variables
- `callback`: Function pointer to the replacement logic that will be called for each matching Var node
- `callback_arg`: Generic pointer to additional context data that the callback function may need
- `target_varno`: The range table entry index that identifies which variables should be processed
- `sublevels_up`: Current nesting depth in subqueries, used to properly handle variable scoping
- `inserted_sublink`: Boolean flag tracking whether any SubLink nodes have been inserted during processing

## Dependencies
- Functions called/Symbols referenced:
  - replace_rte_variables_callback (typedef)
- Called from (representative examples):
  - [replace_rte_variables](replace_rte_variables.md) (src/backend/rewrite/rewriteManip.c:1352)
  - [replace_rte_variables_mutator](replace_rte_variables_mutator.md) (src/backend/rewrite/rewriteManip.c:1394)
  - [ReplaceVarsFromTargetList_callback](../R/ReplaceVarsFromTargetList_callback.md) (src/backend/rewrite/rewriteManip.c:1670)
  - [pullup_replace_vars_callback](../p/pullup_replace_vars_callback.md) (src/backend/optimizer/prep/prepjointree.c:2485)

## Notes and Other Information
This structure is part of PostgreSQL's query rewriting infrastructure and is essential for operations like view expansion, rule processing, and query optimization. The callback-based design allows for different replacement strategies while maintaining a consistent traversal framework. The `inserted_sublink` flag is crucial for maintaining the correct hasSubLinks state in Query nodes after transformation.