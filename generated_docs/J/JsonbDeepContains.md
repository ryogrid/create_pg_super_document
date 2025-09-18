# JsonbDeepContains

## Location
src/backend/utils/adt/jsonb_util.c: 1068 - 1321

## Overview
Implements the core containment logic for PostgreSQL's JSONB "@>" (contains) operator, determining if one JSONB structure is contained within another through recursive comparison.

## Definition


## Detailed Description
JsonbDeepContains implements formal containment semantics defined as "top-down, unordered subtree isomorphism." The function recursively compares two JSONB structures to determine if the second (mContained) is completely contained within the first (val). It handles both object and array containers with different containment rules:

For objects: All key-value pairs in mContained must exist in val with matching values. The lhs object may contain additional pairs not present in rhs.

For arrays: All elements in mContained must be found in val. For nested containers, the function performs O(N^2) comparison of container elements. Arrays support both regular arrays and "raw scalar" pseudo-arrays with special containment rules.

The function includes stack depth checking to prevent overflow from deeply nested structures and uses recursive calls to handle nested containers. Memory management is carefully handled for temporary iterators created during nested comparisons.

## Parameters / Member Variables
- : Double pointer to JsonbIterator for the containing (left-hand side) JSONB structure
- : Double pointer to JsonbIterator for the contained (right-hand side) JSONB structure

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - [JsonbIteratorNext](JsonbIteratorNext.md)
  - [getKeyJsonValueFromContainer](../g/getKeyJsonValueFromContainer.md)
  - IsAJsonbScalar
  - [equalsJsonbScalarValue](../e/equalsJsonbScalarValue.md)
  - [JsonbIteratorInit](JsonbIteratorInit.md)
  - [findJsonbValueFromContainer](../f/findJsonbValueFromContainer.md)
  - [JsonbDeepContains](JsonbDeepContains.md) (recursive call)
  - [palloc](../p/palloc.md), pfree
  - WJB_BEGIN_OBJECT, WJB_BEGIN_ARRAY, WJB_END_OBJECT, WJB_END_ARRAY, WJB_KEY, WJB_VALUE, WJB_ELEM
  - jbvObject, jbvArray, jbvBinary, jbvString
  - JB_FARRAY
- Called from (representative examples):
  - [jsonb_contains](../j/jsonb_contains.md)
  - [jsonb_contained](../j/jsonb_contained.md)
  - [JsonbDeepContains](JsonbDeepContains.md) (recursive calls)

## Notes and Other Information
- Implements PostgreSQL's JSONB containment operator (@>) semantics
- Uses recursive descent with automatic stack depth protection
- Object containment: requires all rhs pairs to exist in lhs with matching values
- Array containment: requires all rhs elements to be found in lhs (O(N^2) for nested containers)
- Supports raw scalar pseudo-arrays with special containment rules
- Handles nested structures through recursive JsonbIteratorInit and JsonbDeepContains calls
- Memory management includes careful cleanup of temporary iterators in nested array comparisons
- Critical performance consideration: nested array containment has quadratic complexity
- The function implements injective mapping requirement for container node relationships