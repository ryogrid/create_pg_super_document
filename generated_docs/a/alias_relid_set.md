# alias_relid_set

## Location
[src/backend/optimizer/util/var.c:1098-1114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L1098-L1114)

## Overview
A utility function that processes a set of range table indexes and expands any join relations to their underlying base and outer join relation IDs.

## Definition

```c
static Relids
alias_relid_set(Query *query, Relids relids)
```
## Detailed Description
The  function takes a bitmap set of range table indexes and processes each one to replace join relations with their constituent base relations and outer join relations. This is essential for query optimization when dealing with joins that need to be decomposed into their underlying relations for proper analysis and planning.

The function iterates through each member of the input relids set. For each range table entry:
- If it's a join relation (RTE_JOIN), it calls  to obtain the underlying base and outer join relation IDs and adds them to the result
- If it's not a join relation, it simply adds the original relation ID to the result

This process effectively "flattens" join aliases by expanding them to show the actual relations involved, which is crucial for various optimizer operations that need to work with the actual base relations rather than join aliases.

## Parameters / Member Variables
- : Pointer to the Query structure containing the range table and other query information
- : Input bitmap set of range table indexes to be processed and potentially expanded

## Dependencies
- Functions called/Symbols referenced:
  - : Iterates through members of the bitmap set
  - : Retrieves range table entry by index
  - : Constant indicating a join range table entry type
  - : Combines two bitmap sets
  - : Gets the underlying relation IDs for a join
  - : Adds a single member to a bitmap set

- Called from (representative examples):
  - : Used in the context structure for flattening join alias variables
  - : Called during the mutation process for flattening join alias variables

## Notes and Other Information
- This is a static function, meaning it's only accessible within the var.c file
- The function is part of PostgreSQL's query optimization infrastructure, specifically dealing with variable reference resolution
- It plays a key role in the process of flattening join alias variables, ensuring that references to join relations are properly expanded to their constituent base relations
- The function handles the bitmap manipulation carefully, building up the result set incrementally as it processes each input relation ID
- Located in src/backend/optimizer/util/var.c at lines 1098-1114

## Simplified Source

```c
static Relids
alias_relid_set(Query *query, Relids relids)
{
    Relids result = NULL;
    int rtindex;

    // Iterate through each relation ID in the input set
    rtindex = -1;
    while ((rtindex = bms_next_member(relids, rtindex)) >= 0) {
        RangeTblEntry *rte = rt_fetch(rtindex, query->rtable);

        // Expand join relations to their underlying base relations
        if (rte->rtekind == RTE_JOIN)
            result = bms_join(result, get_relids_for_join(query, rtindex));
        // Keep non-join relations as-is
        else
            result = bms_add_member(result, rtindex);
    }

    return result;
}
```