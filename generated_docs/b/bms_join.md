# bms_join

## Location
[src/backend/nodes/bitmapset.c:1230-1305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L1230-L1305)

## Overview
Performs union of two bitmap sets with the flexibility to recycle either input bitmap set, choosing the larger one as the base for maximum efficiency.

## Definition

```c
union the shorter input into the result */
	otherlen = other->nwords;
```
## Detailed Description
The bms_join function computes the union of two bitmap sets, similar to bms_union, but with a key optimization: it can recycle either input bitmap set rather than always creating a new one. The function intelligently chooses the larger bitmap set as the result container and unions the smaller set into it, minimizing memory operations and reallocation needs.

This approach is more flexible than functions like bms_add_members or bms_int_members that can only recycle the left operand. By choosing the larger set as the base, bms_join reduces the amount of copying needed and provides better memory efficiency for asymmetric unions.

## Parameters / Member Variables
- : The first bitmap set to union (can be NULL, may be recycled)
- : The second bitmap set to union (can be NULL, may be recycled)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation of both inputs)
  - [bms_copy_and_free](bms_copy_and_free.md) (conditional memory management)
  - [pfree](../p/pfree.md) (memory deallocation of unused input)
  
- Called from (representative examples):
  - [add_paths_to_joinrel](../a/add_paths_to_joinrel.md) (join path creation)
  - [process_equivalence](../p/process_equivalence.md) (equivalence class processing)
  - [finalize_primnode](../f/finalize_primnode.md) (primitive node finalization)
  - build_joinrel_tlist (join relation target list construction)
  - [pull_varnos_walker](../p/pull_varnos_walker.md) (variable number extraction)

## Notes and Other Information
- Returns the non-NULL input if the other input is NULL
- Returns NULL if both inputs are NULL
- Automatically selects the larger bitmap set as the result container for efficiency
- Frees the smaller input bitmap set after union operation
- Uses bitwise OR operation to perform the union
- More memory-efficient than bms_union when one set is significantly larger
- Supports conditional reallocation based on REALLOCATE_BITMAPSETS compile flag
- Extensively used in PostgreSQL's query optimization, particularly in join processing
- The 'pure paranoia' check ensures the smaller set is only freed if it's different from result

## Simplified Source

```c
Bitmapset *
bms_join(Bitmapset *a, Bitmapset *b)
{
    Bitmapset *result;
    Bitmapset *other;
    int otherlen;
    int i;

    Assert(bms_is_valid_set(a));
    Assert(bms_is_valid_set(b));

    // Handle NULL inputs - return the non-NULL one
    if (a == NULL)
        return b;
    if (b == NULL)
        return a;

    // Choose the larger bitmap set as the result to minimize copying
    if (a->nwords < b->nwords)
    {
        result = b;  // b is larger, use it as base
        other = a;   // union a into b
    }
    else
    {
        result = a;  // a is larger or equal, use it as base
        other = b;   // union b into a
    }

    // Union the smaller set into the larger one using bitwise OR
    otherlen = other->nwords;
    i = 0;
    do
    {
        result->words[i] |= other->words[i];
    } while (++i < otherlen);

    // Free the smaller input set (paranoia check ensures it's different)
    if (other != result)
        pfree(other);

    return result;
}
```