# ExecGetInsertNewTuple

## Location
src/backend/executor/nodeModifyTable.c: 697 - 740

## Overview
Prepares a new tuple for insertion by removing junk columns and ensuring the tuple matches the target relation's format.

## Definition


## Detailed Description
This function transforms a tuple from the subplan's output format into a format suitable for insertion into the target relation. It handles two main scenarios:

1. **No Projection Needed**: When the subplan output directly matches the target table format, it optimizes by either using the original slot (if slot types match) or copying to the appropriate slot type
2. **Projection Required**: When junk columns need to be filtered out, it applies the projection to produce a clean tuple

The function includes an important optimization: it avoids unnecessary slot copying when the plan slot already has the correct type for the target relation. This is common in INSERT operations where the subplan output typically doesn't contain junk columns.

Key behavioral notes:
- The projection path is currently dead code for INSERT operations in PostgreSQL, as INSERT typically doesn't receive junk columns
- The function primarily serves as a slot type compatibility layer
- Always returns a slot appropriate for the target relation

## Parameters / Member Variables
- : Result relation information containing projection info and target slot
- : Tuple table slot from the subplan containing the source tuple data

## Dependencies
- Functions called/Symbols referenced:
  - ExecCopySlot
  - ExecProject
- Called from (representative examples):
  - ExecModifyTable

## Notes and Other Information
- This is a static function only used within nodeModifyTable.c
- The projection code path is noted as dead code in current PostgreSQL versions for INSERT operations
- Provides slot type compatibility handling between subplan output and target relation requirements
- Optimizes the common case where no projection or slot copying is needed
- The function design anticipates future scenarios where INSERT might need projection (though currently unused)
- Returns either the original planSlot or the ri_newTupleSlot depending on type compatibility and projection needs