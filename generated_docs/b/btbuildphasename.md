# btbuildphasename

## Location
src/backend/access/nbtree/nbtutils.c: 4609 - 4656

## Overview
The btbuildphasename function returns human-readable names for different phases of B-tree index construction, used for progress reporting during index builds.

## Definition
```c
char *btbuildphasename(int64 phasenum)
```

## Detailed Description
This function provides descriptive names for the various phases of B-tree index construction. It is used by PostgreSQL's progress reporting system to display meaningful status information to users during CREATE INDEX operations. The function maps internal phase numbers to user-friendly strings that describe what operation is currently being performed.

The function supports five distinct phases of B-tree index building:
1. Initialization phase
2. Table scanning phase (reading source data)
3. First sort phase (sorting live tuples)
4. Second sort phase (sorting dead tuples)
5. Tree loading phase (building the actual B-tree structure)

## Parameters / Member Variables
- `phasenum`: 64-bit integer representing the current phase of index construction

## Dependencies
- Functions called/Symbols referenced:
  - PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE (constant)
  - PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN (constant)
  - PROGRESS_BTREE_PHASE_PERFORMSORT_1 (constant)
  - PROGRESS_BTREE_PHASE_PERFORMSORT_2 (constant)
  - PROGRESS_BTREE_PHASE_LEAF_LOAD (constant)
- Called from (representative examples):
  - bthandler

## Notes and Other Information
This function is part of PostgreSQL's progress reporting infrastructure, introduced to provide users with visibility into long-running index creation operations. The phase names returned are designed to be informative for database administrators monitoring index builds. The function returns NULL for unrecognized phase numbers, allowing for graceful handling of unknown phases.