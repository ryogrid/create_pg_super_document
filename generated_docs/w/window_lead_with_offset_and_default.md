# window_lead_with_offset_and_default

## Location
src/backend/utils/adt/windowfuncs.c: 638 - 648

## Overview
The window_lead_with_offset_and_default function implements the LEAD window function with both an offset parameter and a default value, returning the value of a column from a row that is a specified number of positions after the current row, or a default value if no such row exists.

## Definition


## Detailed Description
This function provides the full SQL LEAD window function functionality with both offset and default value capabilities. It retrieves the value of a specified column from a row that is exactly 'offset' rows after the current row within the same partition. If no such row exists (e.g., when near the end of the partition), it returns the specified default value instead. The function delegates its core logic to the common leadlag_common function with parameters indicating it's a lead operation with both offset and default value.

## Parameters / Member Variables
- : Function call information structure containing the arguments and context for the window function call, including the offset and default value

## Dependencies
- Functions called/Symbols referenced:
  - leadlag_common (with parameters true, true, true indicating lead operation, with offset, with default)
- Called from:
  - No direct references found (likely called through PostgreSQL's function call mechanism)

## Notes and Other Information
- Located in src/backend/utils/adt/windowfuncs.c:638-648
- This is a wrapper function that delegates to leadlag_common with specific parameters
- The three boolean parameters to leadlag_common represent: is_lead=true (it's a lead), has_offset=true, has_default=true
- Part of PostgreSQL's SQL window function implementation for LEAD(expr, offset, default)
- This is the most complete version of the LEAD function, supporting all optional parameters
- Counterpart to window_lag_with_offset_and_default, but looks forward instead of backward in the partition