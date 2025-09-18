# zapallsubs

## Location
src/backend/regex/regexec.c: 663 - 678

## Overview
Initializes all subexpression matches in a regmatch_t array to "no match" state, while preserving the overall match location.

## Definition
static void zapallsubs(regmatch_t *p, size_t n)

## Detailed Description
The zapallsubs function is a utility function in PostgreSQL's regex execution engine that resets all subexpression match results to indicate "no match" found. This is accomplished by setting both the start offset (rm_so) and end offset (rm_eo) of each subexpression match to -1. Importantly, the function deliberately leaves p[0] (the overall match location) untouched, as this represents the entire pattern match rather than a subexpression.

The function iterates backwards through the array from index n-1 down to 1, ensuring that all subexpression matches are reset while preserving the main match information.

## Parameters / Member Variables
- p: Pointer to an array of regmatch_t structures representing match locations
- n: Size of the regmatch_t array (number of elements)

## Dependencies
- Functions called/Symbols referenced:
  - regmatch_t (structure type)
- Called from (representative examples):
  - LOFF (macro/function at src/backend/regex/regexec.c:146)
  - LOCALDFA (macro/function at src/backend/regex/regexec.c:237, 245, 323)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the regexec.c file
- The function is part of PostgreSQL's internal regex implementation
- The backward iteration (from n-1 to 1) is an optimization technique
- Setting rm_so and rm_eo to -1 is the standard way to indicate "no match" in POSIX regex
- The preservation of p[0] is crucial as it contains the overall match boundaries