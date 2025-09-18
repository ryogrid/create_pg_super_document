# ExecCheckIndexConstraints

## Location
src/backend/executor/execIndexing.c: 527 - 688

## Overview
Checks whether a tuple violates any unique or exclusion constraints by testing against the relevant indices, primarily used for pre-insertion conflict detection in INSERT ... ON CONFLICT operations.

## Definition
```c
bool ExecCheckIndexConstraints(ResultRelInfo *resultRelInfo, 
                              TupleTableSlot *slot,
                              EState *estate, 
                              ItemPointer conflictTid,
                              List *arbiterIndexes)
```

## Detailed Description
ExecCheckIndexConstraints performs constraint violation checking without actually inserting index tuples, making it suitable for pre-insertion conflict detection. This function is primarily used in INSERT ... ON CONFLICT operations where PostgreSQL needs to determine if a proposed insertion would violate unique or exclusion constraints before attempting the actual insertion.

The function operates by:
1. Iterating through all indices associated with the result relation (or only those specified in arbiterIndexes)
2. Filtering to only examine unique indices and indices with exclusion operators
3. Evaluating partial index predicates to determine applicability
4. Forming index datums from the tuple data
5. Using check_exclusion_or_unique_constraint to test for constraint violations
6. Returning immediately upon finding the first violation, with the conflicting tuple ID stored in conflictTid

Key characteristics:
- Does not perform any locking, so conflicts could appear after this check returns
- Only checks immediate (non-deferrable) constraints - deferrable constraints are explicitly rejected
- Returns early on the first constraint violation found
- Supports both unique constraints and exclusion constraints
- Can be limited to specific arbiter indices for INSERT ... ON CONFLICT operations

## Parameters / Member Variables
- `resultRelInfo`: ResultRelInfo containing opened index relations and metadata for the target table
- `slot`: TupleTableSlot containing the tuple data to be tested for constraint violations  
- `estate`: Executor state providing expression evaluation context and execution environment
- `conflictTid`: Output parameter that receives the ItemPointer of the conflicting tuple if a violation is found
- `arbiterIndexes`: List of index OIDs to check (NIL means check all unique/exclusion indices)

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md): Initializes conflict TID pointers to invalid state
  - GetPerTupleExprContext: Gets expression evaluation context for tuple processing
  - [list_member_oid](../l/list_member_oid.md): Checks if an index OID is in the arbiter list
  - [errtableconstraint](../e/errtableconstraint.md): Reports constraint violation errors with proper error context
  - [ExecPrepareQual](ExecPrepareQual.md): Prepares partial index predicate expressions for evaluation
  - ExecQual: Evaluates partial index predicate expressions
  - [FormIndexDatum](../F/FormIndexDatum.md): Extracts index column values from the heap tuple
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md): Performs the actual constraint checking logic
- Called from (representative examples):
  - [ExecInsert](ExecInsert.md): Used in INSERT ... ON CONFLICT to detect conflicts before insertion
  - nodeModifyTable operations: Part of conflict detection in modification operations

## Notes and Other Information
- This is a read-only operation that does not modify any data or acquire significant locks
- The function explicitly rejects deferrable unique constraints as they are not supported in ON CONFLICT operations
- Returns false immediately upon finding the first constraint violation, making it efficient for conflict detection
- The conflictTid output parameter is only meaningful when the function returns false
- Partial indices are properly handled by evaluating their predicate expressions
- Used primarily in the implementation of INSERT ... ON CONFLICT functionality
- The lack of locking means race conditions are possible, but this is acceptable for its intended use case
- Validation ensures that when specific arbiter indexes are requested, at least one valid index is actually checked