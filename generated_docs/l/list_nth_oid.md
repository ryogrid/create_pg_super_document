# list_nth_oid

## Location
[src/include/nodes/pg_list.h:321-326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L321-L326)

## Overview
Returns the OID (Object Identifier) value contained in the n-th element of an OidList, providing type-safe indexed access to OID values.

## Definition

```c
static inline Oid
list_nth_oid(const List *list, int n)
```
## Detailed Description
The `list_nth_oid` function is a specialized variant of `list_nth` designed specifically for OidList structures that store Object Identifier (OID) values. OIDs are fundamental identifiers used throughout PostgreSQL to uniquely identify database objects such as tables, functions, types, and other catalog entities.

This function ensures type safety by asserting that the provided list is actually an OidList before attempting to access its contents. It provides O(1) access time due to PostgreSQL's array-based list implementation and uses zero-based indexing consistent with other list access functions.

## Parameters / Member Variables
- `list`: A const pointer to the List structure, which must be an OidList containing OID values
- `n`: Zero-based index of the OID element to retrieve (must be within bounds: 0 <= n < list->length)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (for type checking to ensure list is OidList)
  - list_nth_cell (to get the cell at position n)
  - lfirst_oid (to extract the OID value from the cell)
- Called from (representative examples):
  - [interpret_AS_clause](../i/interpret_AS_clause.md) (function parameter processing)
  - EstimateParamExecSpace (parallel execution parameter estimation)
  - [expand_indexqual_rowcompare](../e/expand_indexqual_rowcompare.md) (index qualification expansion)
  - [get_rte_attribute_is_dropped](../g/get_rte_attribute_is_dropped.md) (relation attribute checking)
  - [rewriteSearchAndCycle](../r/rewriteSearchAndCycle.md) (search/cycle rewriting logic)
  - [pg_partition_ancestors](../p/pg_partition_ancestors.md) (partition hierarchy functions)

## Notes and Other Information
- This is a static inline function for optimal performance
- Specifically designed for OidList structures containing PostgreSQL Object Identifiers
- Uses zero-based indexing (first element is at index 0)
- Type safety is enforced through runtime assertion checking
- OIDs are used extensively in PostgreSQL's system catalogs and internal object management
- Commonly used in query optimization, system catalog access, and metadata operations
- No bounds checking beyond what's provided by the underlying list_nth_cell function
- Part of PostgreSQL's type-safe list manipulation API for object identifier management
- Calling with a non-OidList or invalid index will result in assertion failure