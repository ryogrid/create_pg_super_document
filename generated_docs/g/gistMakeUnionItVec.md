# gistMakeUnionItVec

## Location
src/backend/access/gist/gistutil.c: 154 - 217

## Overview
Creates union datums for each index column by combining corresponding column values from a vector of IndexTuples using the GiST union functions.

## Definition
void gistMakeUnionItVec(GISTSTATE *giststate, IndexTuple *itvec, int len, Datum *attr, bool *isnull)

## Detailed Description
This function processes a vector of IndexTuples and creates union datums for each index column. For each column in the index, it collects all non-null values from the IndexTuples, creates GIST entries for them, and then calls the appropriate union function to combine them into a single union datum. The resulting union datums represent the bounding key that encompasses all the input tuples for that column.

The function handles several edge cases: if all values in a column are NULL, the union is marked as NULL; if only one non-null value exists, it duplicates the entry to ensure the union function receives at least two inputs (as some union functions may expect multiple inputs). The resulting datums are stored in the provided attr array with corresponding null flags in the isnull array.

## Parameters / Member Variables
- `giststate`: GIST state structure containing index metadata, function pointers, and tuple descriptors
- `itvec`: Array of IndexTuple pointers to process
- `len`: Number of IndexTuples in the itvec array
- `attr`: Output array to store the resulting union datums for each column
- `isnull`: Output array to store null flags for each column's union datum

## Dependencies
- Functions called/Symbols referenced:
  - GISTSTATE (GiST state structure type)
  - GistEntryVector (structure for holding GIST entries)
  - GISTENTRY (individual GIST entry type)
  - GEVHDRSZ (GistEntryVector header size constant)
  - index_getattr (extracts attribute value from IndexTuple)
  - gistdentryinit (initializes a GIST entry)
  - FunctionCall2Coll (calls a function with collation support)
  - palloc (PostgreSQL memory allocation)
- Called from (representative examples):
  - gistunionsubkeyvec (in gistsplit.c:64)
  - gistunion (in gistutil.c:223)

## Notes and Other Information
- The function processes each index column independently, creating separate union operations for multi-column indexes
- Union datums are returned uncompressed; compression may be applied by the caller if needed
- The function handles the case where union functions might expect at least two inputs by duplicating single entries
- Memory for the GistEntryVector is allocated with extra space for safety ((len + 2) entries)
- This is a key function in GiST index operations, used during node splits and internal key creation