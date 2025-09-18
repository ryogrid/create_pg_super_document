# remove_rel_from_joinlist

## Location
[src/backend/optimizer/plan/analyzejoins.c:676-729](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L676-L729)

## Overview
Recursively removes all occurrences of a target relation ID from a joinlist structure, rebuilding the list while maintaining its hierarchical organization.

## Definition
```c
static List *remove_rel_from_joinlist(List *joinlist, int relid, int *nremoved)
```

## Detailed Description
This function processes a joinlist structure (which can contain nested sublists) to remove all references to a specific relation ID. The function rebuilds the entire list structure rather than modifying it in place, which simplifies the logic but requires more memory allocation.

The function handles two types of joinlist nodes:
1. RangeTblRef nodes - Direct references to relations that are checked against the target relid
2. Nested List nodes - Sublists that require recursive processing

When a matching relation is found, the removal counter is incremented and the node is excluded from the result. Empty sublists resulting from recursive removal are also excluded to maintain a clean structure.

## Parameters / Member Variables
- `joinlist`: The input joinlist structure to process (can contain RangeTblRef nodes and nested Lists)
- `relid`: The relation ID to be removed from the joinlist
- `nremoved`: Pointer to a counter that tracks the number of relations removed (incremented for each removal)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - lappend (appends elements to lists)
  - elog (error logging)
  - nodeTag (gets the node type tag)
  - [remove_rel_from_joinlist](remove_rel_from_joinlist.md) (recursive self-call)
- Called from (representative examples):
  - [remove_useless_joins](remove_useless_joins.md)
  - [remove_rel_from_joinlist](remove_rel_from_joinlist.md) (recursive self-call)

## Notes and Other Information
- This is a static function within analyzejoins.c, serving as an internal utility
- The function is designed for clarity rather than efficiency, as noted in the comments
- Recursive processing allows handling of arbitrarily nested joinlist structures
- The function includes error handling for unrecognized node types
- Empty sublists are filtered out to prevent structural pollution of the result
- The caller is expected to verify that exactly one occurrence was removed by checking the nremoved counter
- Located in src/backend/optimizer/plan/analyzejoins.c at lines 676-729