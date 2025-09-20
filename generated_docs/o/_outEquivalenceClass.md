# _outEquivalenceClass

## Location
[src/backend/nodes/outfuncs.c:455-480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L455-L480)

## Overview
Serializes an EquivalenceClass node to its string representation, following merge chains to output the topmost merged equivalence class with all its optimization metadata and member information.

## Definition

```c
static void
_outEquivalenceClass(StringInfo str, const EquivalenceClass *node)
```
## Detailed Description
The  function serializes EquivalenceClass nodes, which are fundamental data structures in PostgreSQL's query optimization system for tracking sets of expressions that are known to be equal.

The function first traverses any merge chain by following ec_merged pointers to reach the topmost (canonical) equivalence class, ensuring consistent output regardless of which member of a merged set is passed in. It then outputs comprehensive equivalence class information including operator families (ec_opfamilies), collation settings (ec_collation), member expressions (ec_members), source clauses (ec_sources), derived expressions (ec_derives), and various optimization flags.

The output includes security-related fields (ec_min_security, ec_max_security) and optimization hints (ec_has_const, ec_has_volatile, ec_broken) that influence how the query planner uses this equivalence class for optimization decisions.

## Parameters / Member Variables
- : StringInfo buffer where the serialized output is appended
- : Pointer to the EquivalenceClass node to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - WRITE_NODE_TYPE
  - WRITE_NODE_FIELD
  - WRITE_OID_FIELD
  - WRITE_BITMAPSET_FIELD
  - WRITE_BOOL_FIELD
  - WRITE_UINT_FIELD
- Called from (representative examples):
  - (Part of node output dispatch system - called indirectly through nodeToString mechanisms)

## Notes and Other Information
This function is crucial for debugging PostgreSQL's equivalence class optimization system, which enables advanced optimizations like transitive equality inference and redundant join elimination. The merge chain traversal logic (following ec_merged pointers) ensures that the output represents the canonical form of potentially merged equivalence classes, which is essential for consistent debugging output. EquivalenceClass nodes are central to PostgreSQL's constraint propagation and join ordering algorithms, making this serialization function vital for understanding complex query optimization decisions. The comprehensive field output provides visibility into all aspects of equivalence class state that influence optimization behavior.