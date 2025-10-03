# find_join_domain

## Location
[src/backend/optimizer/path/equivclass.c:2420-2448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L2420-L2448)

## Overview
Finds the highest JoinDomain that is completely enclosed within a given set of relation IDs, used in query optimization for handling outer and full joins.

## Definition

```c
static JoinDomain *
find_join_domain(PlannerInfo *root, Relids relids)
```
## Detailed Description
This is a utility function in PostgreSQL's query optimizer that searches through the list of JoinDomains in the PlannerInfo structure to find the most specific (highest) JoinDomain whose relation IDs are completely contained within the provided relid set. JoinDomains are used to track constraints and dependencies related to outer joins and full joins during query planning.

The function performs a linear search through the join_domains list and returns the first JoinDomain whose jd_relids are a subset of the input relids. The search is designed to find the "highest" domain, meaning the most restrictive one that still fits within the given relation set. If no appropriate JoinDomain is found, the function throws an error, as this indicates an internal inconsistency in the optimizer.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global information about the query being planned, including the list of JoinDomains
- `relids`: Bitmapset of relation IDs within which to search for an enclosed JoinDomain
## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_subset](../b/bms_is_subset.md)
  - [JoinDomain](../J/JoinDomain.md) (struct type)
  - elog (for error reporting)
  - lfirst (list iteration macro)
- Called from (representative examples):
  - [reconsider_outer_join_clause](../r/reconsider_outer_join_clause.md)
  - [reconsider_full_join_clause](../r/reconsider_full_join_clause.md)

## Notes and Other Information
- This is a static function, only accessible within the equivclass.c file
- The function includes a comment noting that the search could be avoided by complicating other APIs, but this approach was chosen for simplicity
- The function will always either return a valid JoinDomain pointer or throw an ERROR - it never returns NULL in normal operation
- The ERROR condition indicates a bug in the optimizer logic, as there should always be an appropriate JoinDomain available for any valid relid set

## Simplified Source

```c
static JoinDomain *
find_join_domain(PlannerInfo *root, Relids relids)
{
    ListCell   *lc;

    // Search through all join domains
    foreach(lc, root->join_domains)
    {
        JoinDomain *jdomain = (JoinDomain *) lfirst(lc);

        // Return first domain that is subset of given relids
        if (bms_is_subset(jdomain->jd_relids, relids))
            return jdomain;
    }

    // Should never happen - indicates optimizer bug
    elog(ERROR, "failed to find appropriate JoinDomain");
    return NULL;
}
```