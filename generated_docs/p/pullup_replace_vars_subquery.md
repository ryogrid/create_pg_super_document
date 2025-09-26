# pullup_replace_vars_subquery

## Location
[src/backend/optimizer/prep/prepjointree.c:2787-2813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L2787-L2813)

## Overview
Specialized wrapper for applying pullup variable replacement to subquery nodes, handling the correct sublevels_up adjustment needed for nested Query processing.

## Definition
```c
static Query *pullup_replace_vars_subquery(Query *query,
                                          pullup_replace_vars_context *context)
```

## Detailed Description
This function is a specialized variant of `pullup_replace_vars` designed specifically for processing subquery (Query) nodes during variable replacement. The key difference is that it calls `replace_rte_variables` with `sublevels_up = 1` instead of the usual 0.

This adjustment is necessary because `replace_rte_variables` normally increments `sublevels_up` when entering a Query node, but for subquery processing in the pullup context, we need to start with `sublevels_up = 1` to account for the fact that we're already operating within a subquery context.

The function ensures that variable level adjustments are handled correctly when processing LATERAL subqueries that contain references to the target subquery being pulled up.

## Parameters / Member Variables
- `query`: The Query (subquery) node to process for variable replacement
- `context`: Context structure containing substitution mappings and control flags for the replacement operation

## Dependencies
- Functions called/Symbols referenced:
  - [replace_rte_variables](../r/replace_rte_variables.md)
  - [pullup_replace_vars_callback](pullup_replace_vars_callback.md)
  - [pullup_replace_vars_context](pullup_replace_vars_context.md)
- Called from (representative examples):
  - [replace_vars_in_jointree](../r/replace_vars_in_jointree.md)

## Notes and Other Information
- Uses `sublevels_up = 1` instead of 0 to handle Query node processing correctly
- Passes NULL for the `outer_hasSubLinks` parameter since subqueries handle their own sublink processing
- Specifically designed for LATERAL subquery processing where variable references cross subquery boundaries
- Returns a modified copy of the Query node with all variable references appropriately replaced