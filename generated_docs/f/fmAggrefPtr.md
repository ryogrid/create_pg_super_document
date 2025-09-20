# fmAggrefPtr

## Location
[src/include/fmgr.h:23-28](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fmgr.h#L23-L28)

## Overview
fmAggrefPtr is a typedef that represents a pointer to an Aggref structure, used as a stub reference in the function manager system to avoid including primnodes.h.

## Definition

```c
typedef struct Aggref *fmAggrefPtr;
```
## Detailed Description
fmAggrefPtr is a forward declaration typedef defined in fmgr.h that creates a pointer type to the Aggref structure without requiring the full definition from primnodes.h. The Aggref structure represents aggregate function references in PostgreSQL's parse tree, containing information about aggregate function calls, their arguments, and execution context. By using this typedef, the function manager can reference aggregate nodes without exposing the complete Aggref implementation details, maintaining clean separation of concerns and reducing header dependencies.

## Parameters / Member Variables
- This is a simple typedef with no parameters or member variables

## Dependencies
- Functions called/Symbols referenced:
  - Aggref (struct - forward declaration only)
- Called from (representative examples):
  - AGG_CONTEXT_WINDOW (aggregate context structure)

## Notes and Other Information
- This typedef serves as an abstraction layer to avoid including primnodes.h in fmgr.h
- The actual Aggref structure definition is found in primnodes.h
- Used specifically for aggregate function contexts and window functions
- Part of PostgreSQL's modular header design to minimize compilation dependencies
- Related to aggregate function processing and window function operations