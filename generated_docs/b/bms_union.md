# bms_union

## Location
[src/backend/nodes/bitmapset.c:251-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L251-L291)

## Overview
Creates a new Bitmapset containing all members from both input bitmapsets (set union operation).

## Definition

```c
Bitmapset *
bms_union(const Bitmapset *a, const Bitmapset *b)
```
## Detailed Description
This function performs a bitwise union operation on two Bitmapsets, creating a new Bitmapset that contains all bits that are set in either input set. The function optimizes performance by copying the larger input set first and then ORing the smaller set into it. This approach minimizes the number of word-by-word operations needed. Both input sets remain unmodified, making this a pure functional operation.

## Parameters / Member Variables
- `*a`: First input bitmapset (can be NULL)
- `*b`: Second input bitmapset (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation macro for input bitmapsets)
  - [bms_copy](bms_copy.md) (creates a copy of a bitmapset)

- Called from (representative examples):
  - [ExecGetAllUpdatedCols](../E/ExecGetAllUpdatedCols.md)
  - [generate_join_implied_equalities](../g/generate_join_implied_equalities.md)
  - [make_join_rel](../m/make_join_rel.md)
  - [join_is_removable](../j/join_is_removable.md)
  - [deconstruct_jointree](../d/deconstruct_jointree.md)
  - [finalize_plan](../f/finalize_plan.md)
  - [build_join_rel](build_join_rel.md)
  - [get_joinrel_parampathinfo](../g/get_joinrel_parampathinfo.md)

## Notes and Other Information
- Safely handles NULL inputs by treating NULL as an empty set
- Returns a copy of the non-NULL input when the other input is NULL
- Uses an optimization strategy: copies the larger set first, then unions the smaller one
- Performs bitwise OR operations on corresponding word pairs for efficiency
- Extensively used in PostgreSQL's query optimizer for combining relation sets and join conditions
- The result is a newly allocated Bitmapset that must be freed by the caller using bms_free()
- Critical for join planning, constraint processing, and relation management operations

## Simplified Source

```c
Bitmapset *
bms_union(const Bitmapset *a, const Bitmapset *b)
{
    Bitmapset  *result;
    const Bitmapset *other;
    int         otherlen;
    int         i;

    Assert(bms_is_valid_set(a));
    Assert(bms_is_valid_set(b));

    // Handle NULL inputs
    if (a == NULL)
        return bms_copy(b);
    if (b == NULL)
        return bms_copy(a);

    // Copy the longer set as the result base
    if (a->nwords <= b->nwords)
    {
        result = bms_copy(b);
        other = a;
    }
    else
    {
        result = bms_copy(a);
        other = b;
    }

    // OR the shorter set into the result
    otherlen = other->nwords;
    i = 0;
    do
    {
        result->words[i] |= other->words[i];
    } while (++i < otherlen);

    return result;
}
```