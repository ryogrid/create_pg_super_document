# sortins_cmp

## Location
[src/backend/regex/regc_nfa.c:662-686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L662-L686)

## Overview
A static comparison function used for sorting arc pointers in PostgreSQL's regex NFA (Non-deterministic Finite Automaton) implementation.

## Definition


## Detailed Description
This function compares two arc pointers for sorting purposes in the regex engine's NFA operations. It implements a three-level comparison strategy to establish a consistent ordering of arcs:

1. First compares the source state numbers (from->no)
2. Then compares the character/color codes (co) 
3. Finally compares the arc types

The function is designed as a qsort() compatible comparison function, returning negative, zero, or positive values to indicate the relative ordering of the two arc pointers. The comparison order is optimized based on the likelihood of differences in each field.

## Parameters / Member Variables
- : Pointer to the first arc pointer to compare (cast from const struct arc *const *)
- : Pointer to the second arc pointer to compare (cast from const struct arc *const *)

## Dependencies
- Functions called/Symbols referenced:
  - [arc](../a/arc.md) (struct type)
- Called from (representative examples):
  - [sortins](sortins.md) (src/backend/regex/regc_nfa.c:642)
  - [moveins](../m/moveins.md) (src/backend/regex/regc_nfa.c:832)
  - [copyins](../c/copyins.md) (src/backend/regex/regc_nfa.c:932)
  - [mergeins](../m/mergeins.md) (src/backend/regex/regc_nfa.c:994, 1003, 1030)

## Notes and Other Information
- This is a static function local to the regc_nfa.c file
- The comparison strategy prioritizes fields in order of likelihood to differ for performance
- Used extensively in NFA arc manipulation operations for maintaining consistent arc ordering
- Compatible with standard library qsort() function signature