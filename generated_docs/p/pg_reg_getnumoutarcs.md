# pg_reg_getnumoutarcs

## Location
src/backend/regex/regexport.c: 134 - 154

## Overview
Returns the count of outgoing regular arcs from a specified state in the NFA, automatically traversing and masking LACON arcs.

## Definition
```c
int pg_reg_getnumoutarcs(const regex_t *regex, int st)
```

## Detailed Description
This function counts the number of outgoing regular (non-LACON) arcs from a specified state in the compiled NFA. It uses the `traverse_lacons` helper function to automatically traverse any LACON (Look Ahead Constraints) arcs and count only the reachable regular arcs. This provides a simplified view to external callers by hiding the complexity of LACON arc traversal. The function includes bounds checking for the state parameter and validation of the regex structure.

## Parameters / Member Variables
- `regex`: A pointer to a compiled regular expression structure (`regex_t`) containing the NFA to analyze
- `st`: The state number from which to count outgoing arcs

## Dependencies
- Functions called/Symbols referenced:
  - `regex_t` (structure type)
  - `REMAGIC` (magic number constant)
  - [guts](../g/guts.md) (internal regex structure)
  - [cnfa](../c/cnfa.md) (compiled NFA structure)
  - [traverse_lacons](../t/traverse_lacons.md) (helper function for LACON traversal)
- Called from (representative examples):
  - [regex_arc_t](../r/regex_arc_t.md) (referenced in regexport.h)

## Notes and Other Information
- The function includes validation to ensure the regex pointer is not NULL and has the correct magic number (REMAGIC)
- Returns 0 if the state number is out of bounds (< 0 or >= nstates)
- Uses `traverse_lacons` with NULL arcs array and 0 length to perform counting only
- This is part of the regex export API that provides access to NFA structure information
- LACON arcs are treated as automatically satisfied and traversed transparently
- The function provides a clean interface for analyzing NFA topology without exposing LACON complexity to external code