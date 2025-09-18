# zaptreesubs

## Location
[src/backend/regex/regexec.c:679-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L679-L701)

## Overview
Recursively initializes all subexpression matches within a regular expression subtree to "no match" state.

## Definition
static void zaptreesubs(struct vars *v, struct subre *t)

## Detailed Description
The zaptreesubs function is a recursive utility in PostgreSQL's regex execution engine that traverses a regular expression subtree and resets all capture group matches to indicate "no match" found. It operates on the tree structure of compiled regular expressions, where each subre (subexpression) node may have child nodes and siblings.

The function first checks if the current subtree node has a capture number (capno > 0), and if so, sets the corresponding match result in the pmatch array to -1 (indicating no match). It then recursively calls itself on all child nodes of the current subtree, ensuring that the entire subtree is processed.

This function is typically used when backtracking in regex matching, where previously successful matches need to be invalidated as the engine explores alternative matching paths.

## Parameters / Member Variables
- v: Pointer to a vars structure containing regex execution state and match results
- t: Pointer to a subre structure representing the current subtree node

## Dependencies
- Functions called/Symbols referenced:
  - struct subre (subexpression tree node structure)
  - struct vars (regex execution variables structure)
  - [zaptreesubs](zaptreesubs.md) (recursive self-call)
- Called from (representative examples):
  - LOFF (macro/function at src/backend/regex/regexec.c:147)
  - [ccondissect](../c/ccondissect.md) (function at src/backend/regex/regexec.c:877)
  - [crevcondissect](../c/crevcondissect.md) (function at src/backend/regex/regexec.c:958)
  - [citerdissect](../c/citerdissect.md) (function at src/backend/regex/regexec.c:1250)
  - [creviterdissect](../c/creviterdissect.md) (function at src/backend/regex/regexec.c:1463)

## Notes and Other Information
- This is a static function, only accessible within regexec.c
- The function is recursive, calling itself to traverse the entire subtree
- Only processes capture groups with valid numbers (capno > 0)
- Bounds checking ensures the capture number is within the valid range (n < v->nmatch)
- Part of PostgreSQL's backtracking regex implementation
- The tree traversal visits children before siblings in the regex parse tree
- Used extensively in dissection functions that handle complex regex patterns