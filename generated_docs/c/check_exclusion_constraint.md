# check_exclusion_constraint

## Location
[src/backend/executor/execIndexing.c:915-931](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execIndexing.c#L915-L931)

## Overview
A simplified wrapper function that checks for violations of exclusion constraints on table tuples, designed for external callers who don't need the full complexity of the internal constraint checking mechanism.

## Definition

```c
void
check_exclusion_constraint(Relation heap, Relation index,
						   IndexInfo *indexInfo,
						   ItemPointer tupleid,
						   const Datum *values, const bool *isnull,
						   EState *estate, bool newIndex)
```
## Detailed Description
This function provides a streamlined interface for checking exclusion constraint violations. It's essentially a wrapper around the more comprehensive  function, but with simplified parameters and behavior tailored for external callers. The function validates that a tuple doesn't violate any exclusion constraints defined on the specified index by checking if the tuple's values conflict with existing tuples in the index according to the exclusion operators.

The function uses the  mode internally, which means it will wait for concurrent transactions to complete before making the final determination about constraint violations.

## Parameters / Member Variables
- : The heap relation (table) containing the tuple being checked
- : The index relation that implements the exclusion constraint
- : Metadata information about the index structure and properties
- : Pointer to the tuple identifier (TID) of the tuple being checked
- : Array of Datum values representing the tuple's indexed column values
- : Array of boolean flags indicating which values are NULL
- : Executor state context for the operation
- : Boolean flag indicating whether this is a newly created index

## Dependencies
- Functions called/Symbols referenced:
  - [check_exclusion_or_unique_constraint](check_exclusion_or_unique_constraint.md)
  - IndexInfo
  - CEOUC_WAIT
- Called from (representative examples):
  - [IndexCheckExclusion](../I/IndexCheckExclusion.md)
  - [unique_key_recheck](../u/unique_key_recheck.md)

## Notes and Other Information
- This is explicitly described as a "dumbed down version" for external callers who don't need the special modes available in the full constraint checking function
- The function always uses  mode and passes  for the  parameter to the underlying implementation
- Part of PostgreSQL's constraint enforcement system, specifically handling exclusion constraints which prevent certain combinations of values from coexisting in a table