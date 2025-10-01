# get_relation_data_width

## Location
[src/backend/optimizer/util/plancat.c:1227-1266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1227-L1266)

## Overview
External API wrapper for  that provides the same functionality but handles the relcache entry management internally.

## Definition

```c
int32
get_relation_data_width(Oid relid, int32 *attr_widths)
```
## Detailed Description
The  function serves as a convenient external API for . It takes a relation OID instead of a Relation pointer, handling the relcache entry opening and closing automatically. This function assumes the relation is already locked by the caller, following PostgreSQL's locking conventions.

The function opens the relation using  with NoLock, calls the core  function to perform the actual width calculation, and then closes the relation. This wrapper pattern is common in PostgreSQL to provide both internal and external APIs for the same functionality.

## Parameters / Member Variables
- : The OID of the relation for which to estimate tuple width
- : Optional pointer to a cache array for storing/retrieving previously computed attribute widths (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [get_rel_data_width](get_rel_data_width.md)
- Called from (representative examples):
  - [set_rel_width](../s/set_rel_width.md)
  - [plan_cluster_use_sort](../p/plan_cluster_use_sort.md)

## Notes and Other Information
- This function assumes the relation is already locked by the caller (NoLock parameter)
- It's essentially a convenience wrapper around  for external callers
- The relcache management is handled transparently for the caller
- Returns the same int32 result as the underlying  function

## Simplified Source

```c
int32 get_relation_data_width(Oid relid, int32 *attr_widths) {
    // Open relation with no additional locking (assumes already locked)
    Relation relation = table_open(relid, NoLock);

    // Calculate data width using core function
    int32 result = get_rel_data_width(relation, attr_widths);

    // Close relation and return result
    table_close(relation, NoLock);
    return result;
}
```