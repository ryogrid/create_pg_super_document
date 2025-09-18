# cdissect

## Location
src/backend/regex/regexec.c: 756 - 828

## Overview
Recursively checks backreferences and determines subexpression matches by dissecting a proposed match to identify submatch boundaries for capture nodes.

## Definition
static int cdissect(struct vars *v, struct subre *t, chr *begin, chr *end)

## Detailed Description
The cdissect function is the core recursive dissection function in PostgreSQL's regex execution engine. It processes a subre (subexpression) tree to validate backreferences and identify capture group boundaries within a matched substring. The function assumes that the caller has already verified that the proposed substring matches the node's DFA (Deterministic Finite Automaton).

The function operates on different types of regex nodes through a switch statement:
- Terminal nodes ("="): No action needed, parent handled the matching
- Back references ("b"): Calls cbrdissect to handle backreference matching
- Concatenation ("."): Calls ccondissect or crevcondissect depending on direction
- Alternation ("|"): Calls caltdissect to handle alternative branches
- Iteration ("*"): Calls citerdissect or creviterdissect for repetitions
- Capture nodes ("("): Recursively calls cdissect on child nodes

The function follows strict rules for managing match data, ensuring that capture group locations are properly cleared before recursive calls and saved upon successful matches. If the current node is a capturing node (capno > 0) and matching succeeds, it calls subset() to record the match boundaries.

## Parameters / Member Variables
- v: Pointer to a vars structure containing regex execution state and match results
- t: Pointer to a subre structure representing the current subexpression node
- begin: Pointer to the beginning character of the substring to dissect
- end: Pointer to the end character (exclusive) of the substring to dissect

## Dependencies
- Functions called/Symbols referenced:
  - struct vars, struct subre, chr (core regex data structures)
  - MDEBUG, LOFF (debugging macros)
  - INTERRUPT, STACK_TOO_DEEP (safety checks)
  - REG_OKAY, REG_NOMATCH, REG_ETOOBIG, REG_ASSERT (return codes)
  - [cbrdissect](cbrdissect.md), ccondissect, crevcondissect, caltdissect, citerdissect, creviterdissect (specialized dissection functions)
  - [subset](../s/subset.md) (match recording function)
  - SHORTER, BACKR (flag constants)
- Called from (representative examples):
  - find (function at src/backend/regex/regexec.c:502)
  - cfindloop (function at src/backend/regex/regexec.c:612)
  - [ccondissect](ccondissect.md), crevcondissect, caltdissect, citerdissect, creviterdissect (recursive calls)

## Notes and Other Information
- This is a static function, only accessible within regexec.c
- The function is recursive and can call itself through child nodes
- Implements complex match data management rules to handle backtracking correctly
- Includes safety checks for operation cancellation and stack overflow
- The function assumes DFA matching has already been performed by the caller
- Match failures should only occur when backreferences are present (BACKR flag)
- Part of PostgreSQL's sophisticated regex backtracking implementation
- Critical for handling complex regex patterns with capture groups and backreferences
- The extensive comment block explains the intricate match data management rules