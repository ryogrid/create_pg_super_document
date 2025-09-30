# copy_pathtarget

## Location
[src/backend/optimizer/util/tlist.c:657-680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L657-L680)

## Overview
Creates a shallow copy of a PathTarget structure, duplicating the container and sortgrouprefs array while sharing the underlying expression trees with the original.

## Definition
```c
PathTarget *copy_pathtarget(PathTarget *src)
```

## Detailed Description
This function performs a shallow copy of a PathTarget structure, creating a new PathTarget instance that shares expression trees with the source but has its own independent expression list and sortgrouprefs array. The function first copies all scalar fields using memcpy, then creates a shallow copy of the expressions list using list_copy(). If the source PathTarget has sortgrouprefs, a new array is allocated and the references are copied.

The shallow copy approach means that while the PathTarget structure itself and its container arrays are independent, the actual expression nodes are shared between the original and copy. This is efficient for cases where the expressions themselves don't need modification, but the PathTarget structure (such as its organization or metadata) needs to be altered independently.

## Parameters / Member Variables
- `src`: The source PathTarget structure to be copied

## Dependencies
- Functions called/Symbols referenced:
  - [PathTarget](../P/PathTarget.md) (data structure)
  - makeNode (node creation)
  - memcpy (memory copy)
  - [list_copy](../l/list_copy.md) (list shallow copy)
  - [list_length](../l/list_length.md) (list utility)
  - [palloc](../p/palloc.md) (memory allocation)
- Called from (representative examples):
  - [create_one_window_path](create_one_window_path.md)
  - [apply_scanjoin_target_to_paths](../a/apply_scanjoin_target_to_paths.md)
  - [create_partitionwise_grouping_paths](create_partitionwise_grouping_paths.md)
  - REPARAMETERIZE_CHILD_PATH_LIST

## Notes and Other Information
- Performs a shallow copy - expression trees are shared between original and copy
- The new PathTarget has its own independent expression list and sortgrouprefs array
- Useful when you need to modify PathTarget metadata without affecting the original
- All scalar fields (like cost, width, volatility info) are copied from the source
- The function is declared in src/include/optimizer/tlist.h

## Simplified Source

```c
PathTarget *copy_pathtarget(PathTarget *src) {
    // Create new PathTarget and copy all scalar fields
    PathTarget *dst = makeNode(PathTarget);
    memcpy(dst, src, sizeof(PathTarget));

    // Create shallow copy of expression list
    dst->exprs = list_copy(src->exprs);

    // Copy sortgrouprefs array if present
    if (src->sortgrouprefs) {
        Size nbytes = list_length(src->exprs) * sizeof(Index);
        dst->sortgrouprefs = palloc(nbytes);
        memcpy(dst->sortgrouprefs, src->sortgrouprefs, nbytes);
    }

    return dst;
}
```