# gistMakeUnionKey

## Location
[src/backend/access/gist/gistutil.c:232-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L232-L279)

## Overview
Creates a union datum for a specific index column by combining two GIST entries using the appropriate union function.

## Definition
void gistMakeUnionKey(GISTSTATE *giststate, int attno, GISTENTRY *entry1, bool isnull1, GISTENTRY *entry2, bool isnull2, Datum *dst, bool *dstisnull)

## Detailed Description
This function creates a union of two GIST entries for a single index column (specified by attno). It handles various null combinations: if both entries are null, the result is null; if one entry is null, the non-null entry is duplicated to provide two inputs to the union function; if both entries are non-null, they are used directly. The function uses a stack-allocated GistEntryVector structure to hold exactly two entries, which is then passed to the column-specific union function.

The function uses a clever union-based storage allocation to create a properly sized GistEntryVector on the stack without dynamic memory allocation, improving performance for this common operation.

## Parameters / Member Variables
- `giststate`: GIST state structure containing index metadata and function pointers
- `attno`: Column number (0-based) for which the union is being computed
- `entry1`: First GIST entry to be combined
- `isnull1`: Flag indicating whether entry1 represents a null value
- `entry2`: Second GIST entry to be combined  
- `isnull2`: Flag indicating whether entry2 represents a null value
- `dst`: Output parameter to receive the resulting union datum
- `dstisnull`: Output parameter to receive the null flag for the result

## Dependencies
- Functions called/Symbols referenced:
  - [GISTSTATE](../G/GISTSTATE.md) (GiST state structure type)
  - [GISTENTRY](../G/GISTENTRY.md) (individual GIST entry type)
  - [GistEntryVector](../G/GistEntryVector.md) (structure for holding GIST entries)
  - GEVHDRSZ (GistEntryVector header size constant)  
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (calls union function with collation support)
  - [PointerGetDatum](../P/PointerGetDatum.md) (converts pointer to Datum)
- Called from (representative examples):
  - [supportSecondarySplit](../s/supportSecondarySplit.md) (in gistsplit.c:328, 332)
  - gistgetadjusted (in gistutil.c:335)

## Notes and Other Information
- The function uses a union structure for efficient stack-based allocation of the GistEntryVector with exactly 2 entries
- When one entry is null, the non-null entry is duplicated to ensure the union function receives two inputs, as some union functions may expect multiple values
- The function operates on a single column at a time, unlike gistMakeUnionItVec which processes all columns
- This is a lower-level utility primarily used during index splits and adjustments where precise control over individual column unions is needed
- The stack allocation approach avoids the overhead of palloc/pfree for this frequently called function