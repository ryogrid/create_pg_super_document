# sortouts_cmp

## Location
[src/backend/regex/regc_nfa.c:729-757](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L729-L757)

## Overview
A static comparison function used for sorting arc pointers by destination state, color, and type in PostgreSQL's regex NFA implementation.

## Definition


## Detailed Description
This function compares two arc pointers for sorting outgoing arcs from NFA states. It implements a three-level comparison strategy to establish consistent ordering:

1. First compares the destination state numbers (to->no)
2. Then compares the character/color codes (co)
3. Finally compares the arc types

The function is designed as a qsort() compatible comparison function, returning negative, zero, or positive values to indicate the relative ordering of the two arc pointers. This differs from sortins_cmp by comparing destination states (to->no) instead of source states (from->no).

## Parameters / Member Variables
- : Pointer to the first arc pointer to compare (cast from const struct arc *const *)
- : Pointer to the second arc pointer to compare (cast from const struct arc *const *)

## Dependencies
- Functions called/Symbols referenced:
  - [arc](../a/arc.md) (struct type)
- Called from (representative examples):
  - [sortouts](sortouts.md) (src/backend/regex/regc_nfa.c:709)
  - [moveouts](../m/moveouts.md) (src/backend/regex/regc_nfa.c:1120)
  - [copyouts](../c/copyouts.md) (src/backend/regex/regc_nfa.c:1217)

## Notes and Other Information
- This is a static function local to the regc_nfa.c file
- The comparison strategy prioritizes fields in order of likelihood to differ for performance
- Used specifically for sorting outgoing arcs, complementing sortins_cmp which sorts incoming arcs
- Compatible with standard library qsort() function signature
- The primary difference from sortins_cmp is comparing destination states (to->no) rather than source states (from->no)