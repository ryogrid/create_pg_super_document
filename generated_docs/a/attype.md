# attype

## Location
src/timezone/zic.c: 390 - 410

## Overview
A structure that represents a timezone transition point, containing the time of the transition, its timezone type, and merging control information used by the timezone compiler (zic).

## Definition


## Detailed Description
The  structure is used internally by the timezone compiler to represent transition points in timezone data. Each instance contains information about when a timezone transition occurs (), what timezone type it transitions to (), and whether this transition should be merged with adjacent ones (). The global  array holds all transitions for the timezone being processed and is dynamically allocated and sorted during timezone compilation.

The structure is central to the timezone compilation process, where transitions are collected, sorted chronologically, and then potentially merged to reduce redundancy in the final timezone data file. The merging process helps optimize the size of timezone files by eliminating unnecessary transitions that don't change the effective timezone information.

## Parameters / Member Variables
- : The time at which this timezone transition occurs, represented as a  value
- : A boolean flag indicating whether this transition should be excluded from the merging optimization process
- : An unsigned char identifying the timezone type index that becomes active after this transition

## Dependencies
- Functions called/Symbols referenced:
  - : Time representation type used for the transition timestamp
  - : Maximum number of timezone types (used in related arrays)
- Called from (representative examples):
  - : Comparison function for sorting transitions chronologically
  - : Function that processes transitions to determine observation years

## Notes and Other Information
- The  array is dynamically allocated using  and grows as needed during timezone processing
- Transitions are sorted chronologically using  with the  comparison function
- The merging process controlled by  helps optimize timezone file size by removing redundant transitions
- This structure is only used internally during timezone compilation and doesn't appear in the final timezone data files
- The  field indexes into parallel arrays (, , , etc.) that store timezone type information