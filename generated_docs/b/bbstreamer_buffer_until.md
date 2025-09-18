# bbstreamer_buffer_until

## Location
src/bin/pg_basebackup/bbstreamer.h: 175 - 226

## Overview
This convenience function attempts to accumulate data in a bbstreamer's buffer until it reaches a target length, returning true if the target is achieved and false otherwise.

## Definition


## Detailed Description
bbstreamer_buffer_until is a static inline convenience function designed specifically for use by bbstreamer implementations, not external callers. It provides intelligent buffering logic that attempts to accumulate enough data to reach a specified target length in the bbstreamer's internal buffer.

The function implements three distinct behaviors based on the current state:
1. If the buffer already contains enough data (>= target_bytes), it returns true immediately
2. If the buffer plus all available input data is still insufficient, it buffers all available data and returns false
3. If there's enough total data to reach the target, it buffers exactly what's needed and returns true

This pattern is particularly useful for parsing operations that need to accumulate a fixed amount of data (like headers) before they can proceed with processing.

## Parameters / Member Variables
- : Pointer to the bbstreamer object whose buffer will be used
- : Pointer to a pointer to the input data buffer (modified to advance past buffered bytes)
- : Pointer to the length of remaining input data (modified to reflect consumed bytes)
- : The desired number of bytes to have in the buffer

## Dependencies
- Functions called/Symbols referenced:
  - bbstreamer (struct type)
  - bbstreamer_buffer_bytes (helper function)

- Called from (representative examples):
  - bbstreamer_tar_parser_content

## Notes and Other Information
- This is a static inline function defined in bbstreamer.h for internal use by bbstreamer implementations
- Not intended for use by external callers - it's a utility function for bbstreamer authors
- Returns a boolean indicating whether the target buffer length has been achieved
- Efficiently handles partial data scenarios common in streaming operations
- Modifies input parameters by reference, maintaining consistent interface with bbstreamer_buffer_bytes
- Particularly useful for parsers that need fixed-size headers or chunks before they can process data
- The function optimizes for minimal data copying by only buffering what's necessary to reach the target