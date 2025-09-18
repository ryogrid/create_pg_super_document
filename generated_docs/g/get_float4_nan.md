# get_float4_nan

## Location
src/include/utils/float.h: 111 - 122

## Overview
Returns a single-precision floating-point Not-a-Number (NaN) value using platform-appropriate methods.

## Definition


## Detailed Description
This inline function provides a portable way to obtain a float4 (single-precision floating-point) NaN value. The function uses conditional compilation to choose the most appropriate method based on the platform's capabilities. When the C99 standard NAN macro is available, it uses that for guaranteed portability. Otherwise, it falls back to generating NaN through division by zero (0.0/0.0), which is a well-defined operation that produces NaN according to IEEE 754 floating-point standards.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - NAN (C99 standard macro, when available)
  - float4 (PostgreSQL's single-precision float type)
- Called from (representative examples):
  - float4in_internal (src/backend/utils/adt/float.c:223)
  - numeric_float4 (src/backend/utils/adt/numeric.c:4751)

## Notes and Other Information
- Defined as a static inline function in src/include/utils/float.h:111-122
- Uses conditional compilation (#ifdef NAN) to provide platform compatibility
- The fallback method (0.0/0.0) relies on IEEE 754 floating-point arithmetic standards
- Part of PostgreSQL's internal floating-point utility functions
- Primarily used in numeric conversion routines and input parsing functions