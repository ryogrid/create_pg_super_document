# bmsToString

## Location
src/backend/nodes/outfuncs.c: 808 - 816

## Overview
A utility function that converts a PostgreSQL Bitmapset structure to its ASCII string representation for debugging and logging purposes.

## Definition
char *bmsToString(const Bitmapset *bms)

## Detailed Description
bmsToString is a specialized function designed to convert Bitmapset data structures into human-readable string format. Bitmapsets are commonly used throughout PostgreSQL to represent sets of integers efficiently, particularly for tracking relation sets, attribute sets, and various optimization-related collections. The function creates a StringInfo buffer and uses the outBitmapset function to generate the string representation. This is particularly useful for debugging query optimization, plan generation, and other areas where bitmapsets are heavily utilized.

## Parameters / Member Variables
- bms: A pointer to the Bitmapset structure to be converted to string format (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - outBitmapset
  - initStringInfo
  - StringInfoData (struct)
- Called from (representative examples):
  - search_indexed_tlist_for_var (src/backend/optimizer/plan/setrefs.c:2836)
  - search_indexed_tlist_for_phv (src/backend/optimizer/plan/setrefs.c:2892)

## Notes and Other Information
- This function is specifically designed for Bitmapset structures, unlike the more general nodeToString functions
- Bitmapsets are used extensively in PostgreSQL's query planner and optimizer
- The function handles NULL bitmapsets gracefully through the outBitmapset function
- Memory for the returned string is allocated using PostgreSQL's palloc mechanism
- The string representation typically shows the set members in a readable format, useful for debugging optimizer decisions
- Primarily used in debugging contexts, particularly in the query planner and optimizer components
- The function follows the same StringInfo pattern as other PostgreSQL string-building utilities