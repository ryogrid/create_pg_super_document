# ReindexIndexInfo

## Location
[src/backend/commands/indexcmds.c:3439-3445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L3439-L3445)

## Overview
A local structure used to store information about indexes during concurrent reindexing operations, particularly for tracking index properties needed for safe processing.

## Definition


## Detailed Description
The  structure is a local data structure used within the index reindexing subsystem to collect and organize information about indexes that need to be processed. This structure is particularly important for concurrent reindexing operations where careful coordination and safety checks are required.

The structure captures essential metadata about an index including its OID, the table it belongs to, its access method, and a safety flag that indicates whether the index can be safely processed with certain optimizations or flags enabled.

## Parameters / Member Variables
- : The OID (Object Identifier) of the index being reindexed
- : The OID of the table that owns this index
- : The OID of the access method (AM) used by this index
- : A boolean flag used by  to determine if certain safety optimizations can be applied

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a plain data structure)
- Called from (representative examples):
  - No direct references found (likely used within local functions)

## Notes and Other Information
- This structure appears to be used internally within reindexing functions for organizing index metadata
- The  flag is specifically mentioned in relation to , suggesting it's used for performance or safety optimizations during concurrent operations
- As a typedef struct, it provides a clean interface for passing index information between related functions
- Part of the concurrent reindexing infrastructure where careful tracking of index properties is essential for correctness