# cmpNodePtr

## Location
src/backend/access/spgist/spgtextproc.c: 324 - 332

## Overview
A static comparison function used as a qsort comparator to sort spgNodePtr structures by their 'c' field in ascending order.

## Definition


## Detailed Description
This function serves as a comparator for the qsort library function, specifically designed to sort arrays of spgNodePtr structures. It implements the standard qsort comparator interface by taking two void pointers, casting them to spgNodePtr pointers, and comparing their 'c' fields using PostgreSQL's signed 16-bit integer comparison utility. This function is critical for organizing node pointers in SP-GiST text processing operations where sorting by character values is required.

## Parameters / Member Variables
- : Pointer to the first spgNodePtr structure to compare
- : Pointer to the second spgNodePtr structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - spgNodePtr (structure type)
  - pg_cmp_s16 (PostgreSQL's 16-bit signed integer comparison function)
- Called from (representative examples):
  - spg_text_picksplit (used in qsort operation for sorting node pointers)

## Notes and Other Information
- This is a static function, meaning it has file scope and is only accessible within spgtextproc.c
- The function follows the standard qsort comparator contract: returns negative, zero, or positive values for less-than, equal-to, or greater-than comparisons respectively
- The 'c' field being compared likely represents character values in the context of text processing within SP-GiST indexes