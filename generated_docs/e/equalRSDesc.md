# equalRSDesc

## Location
src/backend/utils/cache/relcache.c: 999 - 1039

## Overview
Determines whether two RowSecurityDesc structures are equivalent by comparing the lists of row security policies they contain.

## Definition


## Detailed Description
This function compares two row security descriptor structures to determine if they contain equivalent sets of row security policies. It handles null pointer cases and compares the policy lists by iterating through them in parallel, using the equalPolicy function to compare individual policies.

The function assumes that RelationBuildRowSecurity builds policies in a consistent order, allowing direct parallel iteration through the policy lists without needing to sort or search.

## Parameters / Member Variables
- : First RowSecurityDesc structure to compare (may be NULL)
- : Second RowSecurityDesc structure to compare (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - RowSecurityDesc (structure type)
  - RowSecurityPolicy (structure type)
  - list_length (function to get list length)
  - forboth (macro for parallel list iteration)
  - lfirst (macro to get list cell value)
  - equalPolicy (function to compare policies)
- Called from (representative examples):
  - RelationClearRelation

## Notes and Other Information
- Returns true if both descriptors are NULL (equivalent empty state)
- Returns false if only one descriptor is NULL
- First checks if policy list lengths are equal before detailed comparison
- Relies on consistent policy ordering from RelationBuildRowSecurity
- Uses forboth macro for efficient parallel iteration through both policy lists
- Part of PostgreSQL's Row Level Security (RLS) system infrastructure
- Used in relation cache management to determine if cached row security information needs to be updated