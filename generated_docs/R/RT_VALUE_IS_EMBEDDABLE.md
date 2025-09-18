# RT_VALUE_IS_EMBEDDABLE

## Location
src/include/lib/radixtree.h: 443 - 462

## Overview
A macro that expands to a function name for determining whether a value can be embedded directly in the child array of a radix tree node rather than allocated separately.

## Definition


## Detailed Description
RT_VALUE_IS_EMBEDDABLE is a macro that generates a function name for checking whether a value can be stored directly within a child pointer slot in the radix tree, rather than requiring separate memory allocation. This is an optimization technique where small values that fit within the size of a pointer can be embedded directly in the tree structure to save memory and improve performance.

The function checks if the size of the value is less than or equal to the size of a pointer allocation slot (RT_PTR_ALLOC). The behavior varies based on compile-time configuration:

- When RT_VARLEN_VALUE_SIZE is defined and RT_RUNTIME_EMBEDDABLE_VALUE is enabled, it performs a runtime check comparing the value size to the pointer size
- When RT_VARLEN_VALUE_SIZE is defined but RT_RUNTIME_EMBEDDABLE_VALUE is disabled, it always returns false (no embedding)  
- When RT_VARLEN_VALUE_SIZE is not defined, it performs a compile-time check of the value size

## Parameters / Member Variables
- : Pointer to the value being checked for embeddability

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME (macro for generating function names)
  - RT_GET_VALUE_SIZE (macro to get the size of a value)
  - RT_PTR_ALLOC (type representing pointer allocation slots)
- Called from (representative examples):
  - RT_SET (within the radix tree set operation at src/include/lib/radixtree.h:1754)

## Notes and Other Information
This function is part of PostgreSQL's generic radix tree implementation template. The RT_ prefix and function name are generated based on the specific instantiation parameters. This optimization allows small values to be stored directly in pointer slots rather than requiring separate memory allocations, which can significantly improve memory efficiency and cache performance for radix trees storing small values.

The embeddability check is crucial for determining the storage strategy in the RT_SET function, where values are either embedded directly in the child pointer slot or allocated as separate leaf nodes.