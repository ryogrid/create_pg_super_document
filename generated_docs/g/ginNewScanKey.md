# ginNewScanKey

## Location
src/backend/access/gin/ginscan.c: 268 - 489

## Overview
Initializes scan key structures for a GIN (Generalized Inverted Index) index scan by processing query values and setting up internal data structures for efficient scanning.

## Definition
```c
void ginNewScanKey(IndexScanDesc scan)
```

## Detailed Description
The `ginNewScanKey` function is a core component of the GIN index scanning infrastructure that processes the scan keys provided by the query planner and transforms them into internal GIN-specific data structures. This function performs several critical operations:

1. **Memory Management**: Allocates scan key information in the key context to ensure proper memory lifecycle management
2. **Query Extraction**: Calls the appropriate `extractQueryFn` for each scan key to extract searchable values from the query arguments
3. **Search Mode Processing**: Handles different GIN search modes (DEFAULT, ALL, EVERYTHING) and applies appropriate logic for each
4. **Null Handling**: Processes null query values and creates appropriate null category representations
5. **Key Reorganization**: Reorders exclude-only keys to appear after normal keys for optimal scanning performance
6. **Version Compatibility**: Ensures compatibility with older GIN index versions and provides appropriate error messages

The function supports various search scenarios including exact matches, partial matches, null searches, and full-index scans. It also handles the complex logic around exclude-only operations and ensures that at least one normal scan key exists when exclude-only keys are present.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure containing the index scan information, including the scan keys to be processed and the opaque scan state

## Dependencies
- Functions called/Symbols referenced:
  - `MemoryContextSwitchTo`: Memory context management
  - `FunctionCall7Coll`: Calls the extractQuery function for each scan key
  - `ginFillScanKey`: Fills in the GIN-specific scan key structure
  - `ginScanKeyAddHiddenEntry`: Adds hidden entries for special search modes
  - `ginGetStats`: Retrieves GIN index statistics for version checking
  - `pgstat_count_index_scan`: Updates index scan statistics
- Called from (representative examples):
  - `gingetbitmap`: Main entry point for GIN bitmap scans

## Notes and Other Information
- This function is called at the beginning of each GIN index scan operation
- The function handles backward compatibility with older GIN index versions (version 0) and will error if unsupported operations are attempted on old indexes
- Memory allocated during this function persists until the scan ends or is rescanned
- The function supports complex query patterns including partial matches and exclude-only operations
- Search modes determine how the scan will behave: DEFAULT for normal equality/containment searches, ALL for exclude-only operations, and EVERYTHING for full-index scans
- The function ensures that exclude-only keys are properly positioned after normal keys in the scan key array for correct execution order