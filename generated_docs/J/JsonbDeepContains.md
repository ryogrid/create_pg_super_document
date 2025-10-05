# JsonbDeepContains

## Location
[src/backend/utils/adt/jsonb_util.c:1068-1321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1068-L1321)

## Overview
Implements the core containment logic for PostgreSQL's JSONB "@>" (contains) operator, determining if one JSONB structure is contained within another through recursive comparison.

## Definition

```c
structure.
				 *
				 * Note that nesting still has to "match up" at the right
				 * nesting sub-levels.  However, there need only be zero or
				 * more matching pairs (or elements) at each nesting level
				 * (provided the *rhs* pairs/elements *all* match on each
				 * level), which enables searching nested structures for a
				 * single String or other primitive type sub-datum quite
				 * effectively (provided the user constructed the rhs nested
				 * structure such that we "know where to look").
				 *
				 * In other words, the mapping of container nodes in the rhs
				 * "vcontained" Jsonb to internal nodes on the lhs is
				 * injective, and parent-child edges on the rhs must be mapped
				 * to parent-child edges on the lhs to satisfy the condition
				 * of containment (plus of course the mapped nodes must be
				 * equal).
				 */
				if (!JsonbDeepContains(&nestval, &nestContained))
					return false;
```
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
  - [check_stack_depth](../c/check_stack_depth.md)
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

## Simplified Source

```c
bool JsonbDeepContains(JsonbIterator **val, JsonbIterator **mContained) {
    JsonbValue vval, vcontained;
    JsonbIteratorToken rval, rcont;

    // Guard against stack overflow from complex JSONB
    check_stack_depth();

    rval = JsonbIteratorNext(val, &vval, false);
    rcont = JsonbIteratorNext(mContained, &vcontained, false);

    // Different container types cannot contain each other
    if (rval != rcont) {
        return false;
    }

    if (rcont == WJB_BEGIN_OBJECT) {
        // Object containment: all rhs pairs must exist in lhs
        if (vval.val.object.nPairs < vcontained.val.object.nPairs)
            return false;

        // Check each key-value pair in contained object
        for (;;) {
            rcont = JsonbIteratorNext(mContained, &vcontained, false);
            if (rcont == WJB_END_OBJECT)
                return true;

            // Find matching key in containing object
            JsonbValue *lhsVal = getKeyJsonValueFromContainer(
                (*val)->container,
                vcontained.val.string.val,
                vcontained.val.string.len,
                &lhsValBuf);
            if (!lhsVal)
                return false;

            // Compare values for matching key
            rcont = JsonbIteratorNext(mContained, &vcontained, true);
            if (lhsVal->type != vcontained.type)
                return false;

            if (IsAJsonbScalar(lhsVal)) {
                if (!equalsJsonbScalarValue(lhsVal, &vcontained))
                    return false;
            } else {
                // Recursively check nested containers
                JsonbIterator *nestval = JsonbIteratorInit(lhsVal->val.binary.data);
                JsonbIterator *nestContained = JsonbIteratorInit(vcontained.val.binary.data);
                if (!JsonbDeepContains(&nestval, &nestContained))
                    return false;
            }
        }
    }
    else if (rcont == WJB_BEGIN_ARRAY) {
        // Array containment: all rhs elements must be found in lhs
        JsonbValue *lhsConts = NULL;
        uint32 nLhsElems = vval.val.array.nElems;

        // Raw scalar can't contain regular array
        if (vval.val.array.rawScalar && !vcontained.val.array.rawScalar)
            return false;

        // Check each element in contained array
        for (;;) {
            rcont = JsonbIteratorNext(mContained, &vcontained, true);
            if (rcont == WJB_END_ARRAY)
                return true;

            if (IsAJsonbScalar(&vcontained)) {
                // Simple scalar search in array
                if (!findJsonbValueFromContainer((*val)->container, JB_FARRAY, &vcontained))
                    return false;
            } else {
                // Complex nested container search (O(N^2))
                if (lhsConts == NULL) {
                    // Initialize array of lhs containers
                    lhsConts = palloc(sizeof(JsonbValue) * nLhsElems);
                    uint32 j = 0;
                    for (uint32 i = 0; i < nLhsElems; i++) {
                        JsonbIteratorNext(val, &vval, true);
                        if (vval.type == jbvBinary)
                            lhsConts[j++] = vval;
                    }
                    if (j == 0) return false;
                    nLhsElems = j;
                }

                // Try to match against each lhs container
                bool found = false;
                for (uint32 i = 0; i < nLhsElems; i++) {
                    JsonbIterator *nestval = JsonbIteratorInit(lhsConts[i].val.binary.data);
                    JsonbIterator *nestContained = JsonbIteratorInit(vcontained.val.binary.data);
                    if (JsonbDeepContains(&nestval, &nestContained)) {
                        found = true;
                        break;
                    }
                }
                if (!found) return false;
            }
        }
    }

    return false; // Should never reach here
}
```