# execTuplesUnequal

## Location
[src/backend/executor/nodeSubplan.c:675-743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubplan.c#L675-L743)

## Overview
execTuplesUnequal determines if two tuples are definitively unequal by comparing specified columns, implementing SQL's NULL semantics where NULLs are neither equal nor unequal to anything.

## Definition
```c
static bool execTuplesUnequal(TupleTableSlot *slot1, TupleTableSlot *slot2, int numCols, AttrNumber *matchColIdx, FmgrInfo *eqfunctions, const Oid *collations, MemoryContext evalContext)
```

## Detailed Description
execTuplesUnequal compares two tuples column-by-column to determine if they can be proven definitively unequal. The function implements SQL's three-valued logic for NULL handling, where the presence of NULL values in either tuple prevents a definitive inequality determination.

Key characteristics of the function:
- Only returns true if there exists at least one non-null column pair that compares unequal
- Skips NULL values in either tuple since they cannot be used to prove inequality
- Uses reverse iteration (from last to first column) as an optimization for sorted data
- Performs comparisons in a temporary memory context to prevent memory leaks
- Supports custom equality functions and collations for each column

The function is primarily used in hash-based subplan execution to determine if tuples with partial NULL matches can be definitively ruled out, supporting efficient implementation of IN/ANY operations with proper NULL semantics.

## Parameters / Member Variables
- `slot1`: First tuple to compare
- `slot2`: Second tuple to compare (must have same column structure as slot1)
- `numCols`: Number of columns to examine for equality
- `matchColIdx`: Array of column numbers to compare (1-based attribute numbers)
- `eqfunctions`: Array of FmgrInfo structures for type-specific equality functions
- `collations`: Array of collation OIDs for each column comparison
- `evalContext`: Temporary memory context for function execution

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextReset](../M/MemoryContextReset.md) (clears temporary evaluation context)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (switches to evaluation context)
  - slot_getattr (extracts column values from tuple slots)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (invokes type-specific equality function with collation)
  - [DatumGetBool](../D/DatumGetBool.md) (converts function result to boolean)
- Called from (representative examples):
  - [findPartialMatch](../f/findPartialMatch.md) (in nodeSubplan.c:758)

## Notes and Other Information
- Implements SQL NULL semantics: NULL values cannot prove inequality
- Optimization: compares columns in reverse order (most significant last) assuming sorted input where later columns are more likely to differ
- Returns false (cannot prove inequality) if all compared columns are NULL in either tuple
- Returns false if all non-null column pairs compare equal
- Returns true only when at least one non-null column pair compares unequal
- Uses temporary memory context to prevent accumulation of memory during equality function calls
- The equality functions are type-specific and support custom collations for text comparisons
- Essential component of partial matching logic in hash-based subplan execution