# appendToBuffer

## Location
src/backend/utils/adt/jsonb_util.c: 1519 - 1532

## Overview
A static utility function that provides a convenient shorthand for reserving space in a StringInfo buffer and then copying data to that reserved space in a single operation.

## Definition


## Detailed Description
The `appendToBuffer` function is a higher-level convenience function that combines two common buffer operations: space reservation and data copying. It first calls `reserveFromBuffer` to allocate the required space at the end of the buffer, then immediately uses `copyToBuffer` to copy the provided data to that newly reserved location. This function streamlines the common pattern of extending a buffer with new data, making JSONB serialization code more readable and less error-prone by eliminating the need for manual offset management.

## Parameters / Member Variables
- `buffer`: A StringInfo structure representing the target buffer where data will be appended
- `data`: Pointer to the source data that will be appended to the buffer
- `len`: The number of bytes to append from the source data

## Dependencies
- Functions called/Symbols referenced:
  - reserveFromBuffer
  - copyToBuffer
- Called from (representative examples):
  - convertJsonbArray
  - convertJsonbObject
  - convertJsonbScalar

## Notes and Other Information
- This is a static function, meaning it's only accessible within the jsonb_util.c compilation unit
- Serves as a convenient wrapper that combines buffer space allocation and data copying into one atomic operation
- Commonly used in JSONB serialization where data needs to be sequentially appended to the output buffer
- The function automatically handles offset calculation by using the return value from reserveFromBuffer
- Provides better code readability compared to manually calling reserveFromBuffer followed by copyToBuffer
- Used extensively in JSONB conversion functions for appending various types of data (scalars, array elements, object members)