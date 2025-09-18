# RT_VERIFY_NODE

## Location
src/include/lib/radixtree.h: 2706 - 2793

## Overview
RT_VERIFY_NODE is a macro that generates the name for a static debugging function that performs sanity checks on radix tree node data structures in PostgreSQL.

## Definition
```c
#define RT_VERIFY_NODE RT_MAKE_NAME(verify_node)
```

The actual function signature when expanded:
```c
static void RT_VERIFY_NODE(RT_NODE *node)
```

## Detailed Description
RT_VERIFY_NODE is an internal debugging function that validates the integrity and consistency of radix tree nodes across all supported node types (RT_NODE_KIND_4, RT_NODE_KIND_16, RT_NODE_KIND_48, and RT_NODE_KIND_256). The function performs type-specific validation checks to ensure that node structures maintain their invariants and are in a valid state.

The function is only active when USE_ASSERT_CHECKING is defined during compilation, making it a debugging-only feature that doesn't impact production performance. Each node type has specific validation rules - for example, RT_NODE_KIND_4 and RT_NODE_KIND_16 verify that their chunk arrays are sorted in ascending order, while RT_NODE_KIND_48 and RT_NODE_KIND_256 validate their bitmap structures and slot usage counts.

This function is extensively used throughout the radix tree implementation during node modifications, growing, and shrinking operations to ensure data structure consistency during development and testing.

## Parameters / Member Variables
- `node`: Pointer to the RT_NODE structure to be validated

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - Assert (debugging assertion macro)
  - [RT_NODE_48_IS_CHUNK_USED](RT_NODE_48_IS_CHUNK_USED.md) (checks if a chunk is used in node48)
  - RT_BM_IDX (bitmap index calculation)
  - RT_BM_BIT (bitmap bit calculation)
  - bmw_popcount (bitmap word population count)

- Called from (representative examples):
  - [RT_ITER](RT_ITER.md) (during iterator operations)
  - [RT_ADD_CHILD_256](RT_ADD_CHILD_256.md) (after adding children to 256-node)
  - [RT_ADD_CHILD_48](RT_ADD_CHILD_48.md) (after adding children to 48-node)
  - [RT_GROW_NODE_16](RT_GROW_NODE_16.md) (after growing 16-node)
  - [RT_ADD_CHILD_16](RT_ADD_CHILD_16.md) (after adding children to 16-node)
  - [RT_GROW_NODE_4](RT_GROW_NODE_4.md) (after growing 4-node)
  - [RT_ADD_CHILD_4](RT_ADD_CHILD_4.md) (after adding children to 4-node)
  - [RT_SHRINK_NODE_48](RT_SHRINK_NODE_48.md) (after shrinking 48-node)
  - [RT_SHRINK_NODE_16](RT_SHRINK_NODE_16.md) (after shrinking 16-node)

## Notes and Other Information
- Only compiled and active when USE_ASSERT_CHECKING is defined
- Performs different validation checks based on node type:
  - RT_NODE_KIND_4 and RT_NODE_KIND_16: Validates chunk arrays are sorted
  - RT_NODE_KIND_48: Validates bitmap consistency and slot usage
  - RT_NODE_KIND_256: Validates bitmap population count matches node count
- Used extensively during node structure modifications to catch data corruption early
- Static function, not accessible outside the radix tree implementation
- Critical for maintaining data structure integrity during development and debugging
- Contains commented RT_DUMP_NODE calls for additional debugging when needed