# get_nullingrels

## Location
[src/backend/optimizer/prep/prepjointree.c:4208-4227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L4208-L4227)

## Overview
Initializes and collects comprehensive information about which outer joins can null which leaf relations throughout the entire query.

## Definition
```c
static nullingrel_info *get_nullingrels(Query *parse)
```

## Detailed Description
This function serves as the entry point for collecting nulling relationship information across the entire query. It creates and initializes a `nullingrel_info` structure that will contain, for each leaf relation in the query, the complete set of outer join relation IDs that can potentially null that relation.

The function performs these key steps:
1. Allocates a new `nullingrel_info` structure
2. Determines the range table length from the query's rtable
3. Allocates a zero-initialized array of `Relids` (bitmap sets) with one entry per range table slot plus one extra
4. Calls `get_nullingrels_recurse` to perform the actual recursive analysis of the join tree

The resulting data structure provides a complete mapping from each base relation to all outer joins that could potentially introduce NULL values for that relation, which is crucial for correct handling of outer join semantics during query optimization and execution.

## Parameters / Member Variables
- `parse`: The Query structure containing the join tree and range table to analyze

## Dependencies
- Functions called/Symbols referenced:
  - palloc_object
  - palloc0_array
  - [get_nullingrels_recurse](get_nullingrels_recurse.md)
  - [list_length](../l/list_length.md)
- Called from (representative examples):
  - [pull_up_simple_subquery](../p/pull_up_simple_subquery.md)

## Notes and Other Information
- This is a static function, accessible only within prepjointree.c
- The function allocates memory for (rtlength + 1) entries to handle 1-based range table indexing
- The array is zero-initialized, meaning relations with no nulling outer joins will have empty bitmap sets
- The actual analysis work is delegated to `get_nullingrels_recurse`, making this function primarily a setup and initialization routine
- The returned structure is essential for maintaining correct NULL semantics during subquery flattening and other query transformations
- Memory allocation uses PostgreSQL's memory management functions (palloc_object, palloc0_array)

## Simplified Source

```c
// Simplified version of get_nullingrels
static nullingrel_info *
get_nullingrels(Query *parse)
{
    nullingrel_info *result = palloc_object(nullingrel_info);

    // Setup data structure for range table length
    result->rtlength = list_length(parse->rtable);
    result->nullingrels = palloc0_array(Relids, result->rtlength + 1);

    // Recursively analyze the join tree to collect nulling relationships
    get_nullingrels_recurse((Node *) parse->jointree, NULL, result);

    return result;
}
```