# addToArray

## Location
src/backend/utils/misc/tzparser.c: 188 - 275

## Overview
Inserts a timezone entry into a sorted array while maintaining alphabetical order and handling duplicate entries.

## Definition
static int addToArray(tzEntry **base, int *arraysize, int n, tzEntry *entry, bool override)

## Detailed Description
This function maintains a dynamically sized array of timezone entries in sorted order by abbreviation name. It uses binary search to locate the correct insertion position and handles duplicate entries according to the override parameter. When duplicates are found, it either preserves identical entries, overrides differing entries if allowed, or reports conflicts. The array is automatically resized when needed using repalloc.

## Parameters / Member Variables
- : Base address of the array (changeable if array must be enlarged)
- : Allocated length of array (changeable if array must be enlarged)  
- : Current number of valid elements in the array
- : New timezone entry data to insert
- : True if OK to override existing entries with same abbreviation

## Dependencies
- Functions called/Symbols referenced:
  - strcmp
  - GUC_check_errmsg
  - GUC_check_errdetail
  - repalloc
  - memmove
  - memcpy
- Called from (representative examples):
  - ParseTzFile

## Notes and Other Information
The function returns the new array length on success, or -1 on error. It uses strcmp() to ensure the sort order matches what datetime.c expects. Duplicate checking considers both the abbreviation and the associated timezone data (offset, zone name, DST flag) to determine if entries are truly identical.