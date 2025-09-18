# WordBoundaryState

## Location
src/backend/utils/adt/formatting.c: 1925 - 1938

## Overview
A state tracking structure used by PostgreSQL's string formatting functions to maintain context for word boundary detection during text processing operations.

## Definition


## Detailed Description
The WordBoundaryState structure is used by PostgreSQL's text formatting functions, particularly for operations like initcap (initial capitalization) that need to identify word boundaries within strings. It maintains the necessary state information to track the current position in a string and determine whether the current character position represents a word boundary based on the alphanumeric properties of adjacent characters. The structure enables efficient sequential processing of strings while maintaining context about the previous character's properties to make boundary decisions.

## Parameters / Member Variables
- : Pointer to the constant character string being processed
- : Total length of the string being processed
- : Current byte offset position within the string
- : Boolean flag indicating whether the state has been initialized
- : Boolean flag indicating whether the previous character was alphanumeric

## Dependencies
- Functions called/Symbols referenced:
  - init (initialization flag)
- Called from (representative examples):
  - [initcap_wbnext](../i/initcap_wbnext.md)
  - [str_initcap](../s/str_initcap.md)

## Notes and Other Information
This structure is specifically designed for text processing operations in PostgreSQL's formatting system (src/backend/utils/adt/formatting.c) that require word boundary detection. It's particularly used by the initcap functionality, which capitalizes the first letter of each word in a string. The structure tracks both the current position in the string and the alphanumeric status of the previous character, which is essential for determining when a new word begins. The init flag ensures proper initialization state management during processing operations.