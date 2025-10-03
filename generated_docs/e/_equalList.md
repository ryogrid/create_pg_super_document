# _equalList

## Location
[src/backend/nodes/equalfuncs.c:156-222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/equalfuncs.c#L156-L222)

## Overview
A static comparison function that determines if two List nodes are equal by comparing their types, lengths, and element-by-element content using specialized comparison logic for different list types.

## Definition

```c
static bool
_equalList(const List *a, const List *b)
```
## Detailed Description
The  function provides comprehensive equality comparison for PostgreSQL's List data structure, which is a fundamental building block used throughout the system for representing ordered collections. The function supports multiple list types (T_List, T_IntList, T_OidList, T_XidList) and employs type-specific comparison strategies for optimal performance.

The function first performs scalar field comparisons to quickly reject unequal lists by checking type and length. Then it uses a switch statement placed outside the iteration loop for efficiency, comparing elements according to their specific data types. For generic T_List types, it recursively calls the general  function on each element, while for specialized types (integers, OIDs, XIDs), it performs direct value comparisons.

## Parameters / Member Variables
- `*a`: Pointer to the first List node to compare
- `*b`: Pointer to the second List node to compare
## Dependencies
- Functions called/Symbols referenced:
  -  (macro for comparing scalar fields)
  -  (macro for parallel iteration over two lists)
  -  (general node equality comparison function)
  -  (macro to get current list element as pointer)
  -  (macro to get current list element as integer)
  -  (macro to get current list element as OID)
  -  (macro to get current list element as XID)
  -  (error logging function)
- Called from (representative examples):
  -  (general equality function at src/backend/nodes/equalfuncs.c:253)

## Notes and Other Information
- This function is marked as , meaning it's only accessible within the equalfuncs.c file
- Handles four distinct list types: T_List (generic node pointers), T_IntList (integers), T_OidList (object IDs), and T_XidList (transaction IDs)
- The switch statement is placed outside the loop for performance optimization
- Uses specialized macros (lfirst_int, lfirst_oid, lfirst_xid) for type-safe element access
- Includes assertions to verify that both lists are fully consumed after comparison
- For T_List elements, delegates to the general  function for recursive comparison
- Specialized list types use direct value comparison for better performance
- The  macro provides parallel iteration over both lists simultaneously
- Lists are ubiquitous in PostgreSQL's node system, representing everything from target lists to join conditions
- The function represents a critical optimization point since list comparisons are frequent in query planning and execution

## Simplified Source

```c
// Simplified version of _equalList
static bool _equalList(const List *a, const List *b) {
    const ListCell *item_a;
    const ListCell *item_b;

    // Quick scalar checks before element-by-element comparison
    COMPARE_SCALAR_FIELD(type);
    COMPARE_SCALAR_FIELD(length);

    // Type-specific comparison for efficiency
    switch (a->type) {
        case T_List:
            forboth(item_a, a, item_b, b) {
                if (!equal(lfirst(item_a), lfirst(item_b))) {
                    return false;
                }
            }
            break;

        case T_IntList:
            forboth(item_a, a, item_b, b) {
                if (lfirst_int(item_a) != lfirst_int(item_b)) {
                    return false;
                }
            }
            break;

        case T_OidList:
            forboth(item_a, a, item_b, b) {
                if (lfirst_oid(item_a) != lfirst_oid(item_b)) {
                    return false;
                }
            }
            break;

        case T_XidList:
            forboth(item_a, a, item_b, b) {
                if (lfirst_xid(item_a) != lfirst_xid(item_b)) {
                    return false;
                }
            }
            break;

        default:
            elog(ERROR, "unrecognized list node type: %d", (int) a->type);
            return false;
    }

    // Verify both lists fully consumed
    Assert(item_a == NULL);
    Assert(item_b == NULL);

    return true;
}
```

Key simplifications made:
- Preserved complete type-specific comparison logic
- Maintained performance optimization with switch outside loop
- Kept specialized accessor macros for different list types
- Focused on core list equality checking functionality