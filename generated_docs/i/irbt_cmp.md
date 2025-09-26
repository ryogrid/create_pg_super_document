# irbt_cmp

## Location
[src/test/modules/test_rbtree/test_rbtree.c:39-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_rbtree/test_rbtree.c#L39-L51)

## Overview
A static node comparator function used by the Red-Black Tree test module to compare IntRBTreeNode instances based on their integer key values.

## Definition

```c
static int
irbt_cmp(const RBTNode *a, const RBTNode *b, void *arg)
```
## Detailed Description
This function serves as the comparator callback for integer-based Red-Black Tree nodes in PostgreSQL's test infrastructure. It implements a simple integer comparison by casting generic RBTNode pointers to IntRBTreeNode structures and comparing their key fields. The function returns a negative value if the first node's key is smaller, zero if equal, and positive if larger, following standard comparison function conventions.

The implementation uses direct subtraction for comparison, with a noted assumption that test keys are non-negative to avoid integer overflow issues.

## Parameters / Member Variables
- : Pointer to the first RBTNode to compare (cast to IntRBTreeNode internally)
- : Pointer to the second RBTNode to compare (cast to IntRBTreeNode internally)  
- : Unused argument parameter (required by RBTNode comparator interface)

## Dependencies
- Functions called/Symbols referenced:
  - RBTNode (generic Red-Black Tree node type)
  - IntRBTreeNode (integer-specific node structure)
- Called from (representative examples):
  - create_int_rbtree (used as comparator function in tree creation)

## Notes and Other Information
- This is a static function used only within the test_rbtree module
- The function assumes non-negative key values to avoid integer overflow in subtraction
- Part of PostgreSQL's Red-Black Tree testing framework located in src/test/modules/test_rbtree/
- Follows the standard three-way comparison pattern (-1, 0, +1) for tree ordering