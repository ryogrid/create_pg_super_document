# bms_del_members

## Location
[src/backend/nodes/bitmapset.c:1161-1229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L1161-L1229)

## Overview
Removes all members from bitmap set 'a' that are also present in bitmap set 'b', recycling the left input bitmap set when possible.

## Definition

```c
Bitmapset *
bms_del_members(Bitmapset *a, const Bitmapset *b)
```
## Detailed Description
The bms_del_members function performs a set difference operation, removing from bitmap set 'a' all members that are also present in bitmap set 'b'. This is equivalent to computing A - B (A minus B) in set theory. The function is optimized to recycle the left input bitmap set rather than creating a new one, similar to bms_int_members.

The function uses bitwise operations to efficiently remove bits, applying the bitwise AND with the complement of b's bits (~b->words[i]). It handles different cases based on the relative sizes of the two bitmap sets and includes optimizations for trailing zero word removal when necessary.

## Parameters / Member Variables
- : The source bitmap set from which members will be removed (can be NULL)
- : The bitmap set containing members to be removed from 'a' (const, not modified, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation of both inputs)
  - [bms_copy_and_free](bms_copy_and_free.md) (conditional memory management)
  - [pfree](../p/pfree.md) (memory deallocation when result becomes empty)
  
- Called from (representative examples):
  - [make_outerjoininfo](../m/make_outerjoininfo.md) (outer join information processing)
  - [check_index_predicates](../c/check_index_predicates.md) (index predicate analysis)
  - [get_join_domain_min_rels](../g/get_join_domain_min_rels.md) (join domain relation calculation)
  - [finalize_plan](../f/finalize_plan.md) (plan finalization)
  - [build_join_rel](build_join_rel.md) (join relation construction)

## Notes and Other Information
- Returns NULL if 'a' is NULL or if the result becomes empty after deletion
- If 'b' is NULL, returns 'a' unchanged (nothing to delete)
- Modifies and potentially frees the left input bitmap set (a)
- Optimizes for cases where 'a' has more words than 'b' (no trailing word removal needed)
- When 'a' has fewer or equal words than 'b', tracks and removes trailing zero words
- Uses bitwise AND with complement (~b->words[i]) for efficient bit removal
- The right operand (b) is never modified (marked const)
- Supports conditional reallocation based on REALLOCATE_BITMAPSETS compile flag
- Extensively used in PostgreSQL's query optimization and join planning

## Simplified Source

```c
Bitmapset *
bms_del_members(Bitmapset *a, const Bitmapset *b)
{
    int i;

    // Handle NULL cases
    if (a == NULL)
        return NULL;
    if (b == NULL)
        return a;

    // Remove b's bits from a using bitwise AND with complement
    if (a->nwords > b->nwords)
    {
        // a is longer than b, no need to trim trailing zeros
        for (i = 0; i < b->nwords; i++)
        {
            a->words[i] &= ~b->words[i];  // Remove bits present in b
        }
    }
    else
    {
        // a is same size or smaller, may need to trim trailing zeros
        int lastnonzero = -1;

        for (i = 0; i < a->nwords; i++)
        {
            a->words[i] &= ~b->words[i];  // Remove bits present in b

            // Track last non-zero word for trimming
            if (a->words[i] != 0)
                lastnonzero = i;
        }

        // Check if result became empty
        if (lastnonzero == -1)
        {
            pfree(a);
            return NULL;
        }

        // Trim trailing zero words
        a->nwords = lastnonzero + 1;
    }

    return a;
}
```