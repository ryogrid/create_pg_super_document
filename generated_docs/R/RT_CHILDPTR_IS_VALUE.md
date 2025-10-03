# RT_CHILDPTR_IS_VALUE

## Location
[src/include/lib/radixtree.h:463-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L463-L490)

## Overview
A macro that expands to a function name for determining whether a child pointer in a radix tree contains an embedded value directly or points to a separate leaf node.

## Definition

```c
static inline bool
RT_CHILDPTR_IS_VALUE(RT_PTR_ALLOC child)
```
## Detailed Description
RT_CHILDPTR_IS_VALUE is a macro that generates a function name for checking whether a child pointer in the radix tree contains an embedded value or points to a separate leaf node. This function complements RT_VALUE_IS_EMBEDDABLE by checking the runtime state of already-stored pointers.

The function's behavior depends on compile-time configuration:

- When RT_VARLEN_VALUE_SIZE and RT_RUNTIME_EMBEDDABLE_VALUE are both defined, it checks a tag bit in the pointer to determine if the value is embedded. In shared memory mode (RT_SHMEM), it checks bit 0 of the child pointer directly. In local memory mode, it checks bit 0 of the pointer cast to uintptr_t.
- When RT_VARLEN_VALUE_SIZE is defined but RT_RUNTIME_EMBEDDABLE_VALUE is disabled, it always returns false (no embedded values)
- When RT_VARLEN_VALUE_SIZE is not defined, it performs a compile-time check comparing the value type size to the pointer allocation size

## Parameters / Member Variables
- `child`: The child pointer (RT_PTR_ALLOC) to check for embedded value
## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - RT_PTR_ALLOC (type representing pointer allocation slots)
  - RT_VALUE_TYPE (the value type stored in the radix tree)
- Called from (representative examples):
  - [RT_FIND](RT_FIND.md) (at src/include/lib/radixtree.h:1123)
  - [RT_SET](RT_SET.md) (at src/include/lib/radixtree.h:1757, 1776)
  - [RT_FREE_RECURSE](RT_FREE_RECURSE.md) (at multiple locations for cleanup)
  - [RT_ITERATE_NEXT](RT_ITERATE_NEXT.md) (at src/include/lib/radixtree.h:2234)
  - [RT_DELETE_RECURSIVE](RT_DELETE_RECURSIVE.md) (at src/include/lib/radixtree.h:2622)

## Notes and Other Information
This function is essential for the radix tree's optimization strategy of embedding small values directly in pointer slots. When RT_RUNTIME_EMBEDDABLE_VALUE is enabled, the lowest bit of the pointer is used as a tag to indicate whether the pointer contains an embedded value or points to a separate leaf node.

The function is used throughout the radix tree operations to determine how to interpret child pointers - whether to treat them as embedded values or dereference them as pointers to leaf nodes. This distinction is crucial for proper memory management and value retrieval in the radix tree implementation.