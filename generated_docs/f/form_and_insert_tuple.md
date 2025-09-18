# form_and_insert_tuple

## Location
src/backend/access/brin/brin.c: 1976 - 1996

## Overview
Converts a deformed tuple stored in the build state into the on-disk format and inserts it into the BRIN index, making the revmap point to the newly inserted tuple.

## Definition
static void form_and_insert_tuple(BrinBuildState *state)

## Detailed Description
This function serves as a key component in the BRIN index construction process. It takes the current in-memory deformed tuple data (bs_dtuple) from the build state and performs the complete process of converting it to the on-disk format and inserting it into the index. The function operates on the current range being processed (bs_currRangeStart) and handles the entire insertion workflow including memory management.

The function performs three main operations:
1. Forms an on-disk tuple using brin_form_tuple() which converts the in-memory summary data into a serialized format
2. Inserts the tuple into the index using brin_doinsert() which handles the physical storage and revmap updates
3. Updates the tuple counter and cleans up allocated memory

## Parameters / Member Variables
- : A BrinBuildState structure containing all necessary context for BRIN index construction, including:
  - bs_bdesc: BRIN descriptor with index metadata
  - bs_currRangeStart: Block number of the current range being processed
  - bs_dtuple: In-memory deformed tuple containing summary data
  - bs_irel: Index relation
  - bs_pagesPerRange: Number of pages per BRIN range
  - bs_rmAccess: Revmap access structure
  - bs_currentInsertBuf: Current insertion buffer
  - bs_numtuples: Counter for inserted tuples

## Dependencies
- Functions called/Symbols referenced:
  - [brin_form_tuple](../b/brin_form_tuple.md): Converts in-memory BRIN summary data into serialized format
  - [brin_doinsert](../b/brin_doinsert.md): Handles the physical insertion of tuple into index
  - [pfree](../p/pfree.md): Memory deallocation function
  - [BrinBuildState](../B/BrinBuildState.md): Build state structure type
  - [BrinTuple](../B/BrinTuple.md): On-disk tuple structure type

- Called from (representative examples):
  - [brinbuildCallback](../b/brinbuildCallback.md): Main callback function during BRIN index build
  - [brinbuild](../b/brinbuild.md): Main BRIN index construction function

## Notes and Other Information
- This is a static function, only accessible within the brin.c file
- The function automatically increments the tuple counter (bs_numtuples) after successful insertion
- Memory allocated by brin_form_tuple() is properly freed using pfree() to prevent memory leaks
- The function assumes that the build state contains valid deformed tuple data ready for conversion
- Part of the BRIN index build pipeline that processes block ranges sequentially