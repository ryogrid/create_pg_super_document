# BuildIndexInfo

## Location
src/backend/catalog/index.c: 2404 - 2462

## Overview
Constructs an IndexInfo record for an open index relation, extracting and organizing all metadata needed for index operations and tuple insertion.

## Definition
IndexInfo *BuildIndexInfo(Relation index)

## Detailed Description
This function builds a comprehensive IndexInfo structure that contains all the essential information about an index required for various index operations, particularly for FormIndexDatum which is used in both index building and individual tuple insertion. The IndexInfo serves as a cached metadata structure that avoids repeated lookups of index properties during bulk operations.

The function extracts information from the index relation's pg_index catalog entry and associated structures:

1. **Basic Attributes**: Copies attribute numbers, key attribute counts, and access method information
2. **Expressions**: Retrieves any index expressions for functional indexes using RelationGetIndexExpressions
3. **Predicates**: Fetches partial index predicate expressions using RelationGetIndexPredicate  
4. **Properties**: Extracts flags for uniqueness, null handling, readiness, and access method capabilities
5. **Exclusion Constraints**: For exclusion indexes, retrieves operator classes, procedures, and strategy numbers

The resulting IndexInfo structure is designed to be long-lived and reused across multiple operations within a command, providing efficient access to index metadata without repeated catalog lookups.

## Parameters / Member Variables
- : An open Relation structure representing the index for which to build IndexInfo

## Dependencies
- Functions called/Symbols referenced:
  - [makeIndexInfo](../m/makeIndexInfo.md)
  - RelationGetRelid
  - [RelationGetIndexExpressions](../R/RelationGetIndexExpressions.md)
  - [RelationGetIndexPredicate](../R/RelationGetIndexPredicate.md)
  - [RelationGetExclusionInfo](../R/RelationGetExclusionInfo.md)
  - INDEX_MAX_KEYS
  - Form_pg_index
- Called from (representative examples):
  - [brinsummarize](../b/brinsummarize.md)
  - [_brin_parallel_scan_and_build](../b/_brin_parallel_scan_and_build.md)
  - [_bt_parallel_scan_and_sort](../b/_bt_parallel_scan_and_sort.md)
  - index_concurrently_create_copy
  - index_concurrently_build
  - [validate_index](../v/validate_index.md)
  - [reindex_index](../r/reindex_index.md)
  - [DefineIndex](../D/DefineIndex.md)
  - [ExecOpenIndices](../E/ExecOpenIndices.md)

## Notes and Other Information
- Validates that the number of index attributes is within valid bounds (1 to INDEX_MAX_KEYS)
- The IndexInfo structure is typically built once per command and reused for potentially many tuple operations
- For exclusion constraints, populates additional arrays (ii_ExclusionOps, ii_ExclusionProcs, ii_ExclusionStrats) with constraint-specific information
- The function assumes the index relation is already properly opened and locked by the caller
- Index expressions and predicates are stored as parsed expression trees, ready for execution
- The ii_Concurrent flag is explicitly set to false, indicating this is for normal (non-concurrent) operations
- Access method summarizing capability is copied from the index access method structure
- Used extensively throughout the system for any operation that needs to work with index metadata efficiently