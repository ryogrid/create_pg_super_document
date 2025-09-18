# diinsert

## Location
src/test/modules/dummy_index_am/dummy_index_am.c: 166 - 179

## Overview
A dummy tuple insertion function that simulates inserting a new tuple into an index without performing any actual insertion.

## Definition
```c
static bool diinsert(Relation index, Datum *values, bool *isnull,
                     ItemPointer ht_ctid, Relation heapRel,
                     IndexUniqueCheck checkUnique,
                     bool indexUnchanged,
                     IndexInfo *indexInfo)
```

## Detailed Description
This function implements the tuple insertion interface for PostgreSQL's dummy index access method. As part of a testing framework, it accepts all the parameters required for index tuple insertion but performs no actual work. The function always returns false, indicating that no insertion was performed. This behavior is consistent with the dummy nature of this access method, which is designed for testing and demonstration rather than actual data storage.

## Parameters / Member Variables
- `index`: Relation representing the index into which the tuple should be inserted
- `values`: Array of Datum values representing the indexed column values
- `isnull`: Array of boolean flags indicating which values are NULL
- `ht_ctid`: ItemPointer to the heap tuple being indexed (tuple identifier)
- `heapRel`: Relation representing the heap table containing the tuple
- `checkUnique`: IndexUniqueCheck enum indicating how to handle uniqueness constraints
- `indexUnchanged`: Boolean flag indicating whether the index values changed since the last update
- `indexInfo`: IndexInfo structure containing metadata about the index

## Dependencies
- Functions called/Symbols referenced:
  - IndexUniqueCheck (enumeration type for uniqueness checking)
  - IndexInfo (index information structure type)
  - ItemPointer (tuple identifier type)
  - Datum (PostgreSQL data value type)
- Called from (representative examples):
  - [dihandler](dihandler.md) (dummy index access method handler at src/test/modules/dummy_index_am/dummy_index_am.c:306)

## Notes and Other Information
- This function is part of PostgreSQL's test infrastructure for the dummy index access method
- The function is declared as static, limiting its scope to the compilation unit
- Always returns false, indicating no tuple was inserted, which is appropriate for a dummy implementation
- Implements the standard PostgreSQL index AM interface for tuple insertion
- Located in src/test/modules/dummy_index_am/dummy_index_am.c:166-179
- Serves as a template for implementing actual tuple insertion in custom access methods
- The function signature matches the standard PostgreSQL index access method interface