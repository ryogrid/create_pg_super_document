# bms_get_singleton_member

## Location
[src/backend/nodes/bitmapset.c:715-750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L715-L750)

## Overview
Tests whether a bitmapset is a singleton and if so, returns the value of its sole member through an output parameter, providing a convenient and efficient alternative to separate membership testing.

## Definition
```c
bool bms_get_singleton_member(const Bitmapset *a, int *member)
```

## Detailed Description
This function combines singleton testing and member retrieval into a single operation. It checks whether the given bitmapset contains exactly one member, and if so, stores that member's value in the provided output parameter and returns true. If the set is empty or contains multiple members, it returns false without modifying the output parameter. This approach is more convenient and efficient than calling bms_membership() followed by bms_singleton_member() when distinguishing between empty and multiple-member sets is not needed.

## Parameters / Member Variables
- `a`: The bitmapset to test for singleton status (const Bitmapset *)
- `member`: Output parameter to receive the singleton member value if found (int *)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md)
  - bitmapword
  - HAS_MULTIPLE_ONES
  - BITS_PER_BITMAPWORD
  - bmw_rightmost_one_pos
- Called from (representative examples):
  - [set_base_rel_consider_startup](../s/set_base_rel_consider_startup.md) (src/backend/optimizer/path/allpaths.c:270)
  - [find_single_rel_for_clauses](../f/find_single_rel_for_clauses.md) (src/backend/optimizer/path/clausesel.c:566)
  - [generate_base_implied_equalities_no_const](../g/generate_base_implied_equalities_no_const.md) (src/backend/optimizer/path/equivclass.c:1226)
  - [join_is_removable](../j/join_is_removable.md) (src/backend/optimizer/plan/analyzejoins.c:177)
  - [create_lateral_join_info](../c/create_lateral_join_info.md) (src/backend/optimizer/plan/initsplan.c:599)
  - [add_placeholders_to_base_rels](../a/add_placeholders_to_base_rels.md) (src/backend/optimizer/util/placeholder.c:339)

## Notes and Other Information
- Returns false for NULL (empty) bitmapsets without error
- Returns false for multi-member sets without error
- Only modifies the output parameter *member when returning true
- More efficient than separate membership testing and member retrieval
- Commonly used in query optimization for singleton relation checks
- Uses the same bit manipulation techniques as bms_singleton_member but with graceful failure handling
- Located in src/backend/nodes/bitmapset.c:715-750

## Simplified Source

```c
bool bms_get_singleton_member(const Bitmapset *a, int *member)
{
    int result = -1;
    int wordnum = 0;

    // Empty set case
    if (a == NULL)
        return false;

    // Check each word in the bitmapset
    do {
        bitmapword w = a->words[wordnum];

        if (w != 0) {
            // If we already found bits or this word has multiple bits
            if (result >= 0 || HAS_MULTIPLE_ONES(w))
                return false;

            // Calculate bit position: word index * bits per word + rightmost bit
            result = wordnum * BITS_PER_BITMAPWORD + bmw_rightmost_one_pos(w);
        }
    } while (++wordnum < a->nwords);

    // Store the singleton member and return success
    *member = result;
    return true;
}
```