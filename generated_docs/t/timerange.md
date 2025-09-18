# timerange

## Location
src/timezone/zic.c: 2045 - 2054

## Overview
The  struct represents a range of time periods and leap second information used in PostgreSQL's timezone compiler for optimizing timezone data output.

## Definition


## Detailed Description
The  structure is used in PostgreSQL's timezone compiler (zic.c) to define and manage time ranges for timezone data optimization. It encapsulates information about time periods, including a default timezone type, base indices, counts for regular time transitions, and separate handling for leap second data. This structure is primarily used during the timezone data compilation process to efficiently organize and limit the scope of timezone information that gets written to the final timezone files.

The structure supports range-based operations on timezone data, allowing the compiler to focus on specific time periods and optimize the output by including only relevant timezone transitions and leap second information for the specified range.

## Parameters / Member Variables
- : Integer specifying the default timezone type to use for this time range
- : Starting index (as ptrdiff_t) in the timezone data arrays for this range
- : Number of time transition entries (as ptrdiff_t) included in this range
- : Starting index for leap second data within this time range
- : Number of leap second entries included in this time range

## Dependencies
- Functions called/Symbols referenced:
  - [timerange](timerange.md) (self-reference in structure definitions)

- Called from (representative examples):
  - [timerange_option](timerange_option.md) (command-line option processing)
  - [limitrange](../l/limitrange.md) (range limitation function)
  - [writezone](../w/writezone.md) (timezone data writing function)

## Notes and Other Information
- Used primarily during timezone compilation to optimize output data size
- Supports selective inclusion of timezone transitions within specified time ranges
- Handles both regular timezone transitions and leap second data separately
- The ptrdiff_t type for base and count allows for large array indexing
- Essential for the -L (limit range) functionality in the zic timezone compiler
- Helps reduce timezone file sizes by excluding historical data outside the specified range
- Used in conjunction with command-line options to control the scope of generated timezone data