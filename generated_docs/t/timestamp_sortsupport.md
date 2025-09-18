# timestamp_sortsupport

## Location
src/backend/utils/adt/timestamp.c: 2291 - 2308

## Overview
The timestamp_sortsupport function provides sort support functionality for timestamp data types by configuring an appropriate comparator function based on the build configuration.

## Definition


## Detailed Description
This function implements PostgreSQL's sort support interface for timestamp values. It examines the build configuration to determine the most efficient comparison method. On platforms where SIZEOF_DATUM >= 8 (64-bit builds with pass-by-value timestamps), it uses the standard signed datum comparator (ssup_datum_signed_cmp). On smaller platforms where timestamps are passed by reference, it falls back to the specialized timestamp_fastcmp function. This conditional approach optimizes sorting performance based on the underlying timestamp representation.

## Parameters / Member Variables
- : A SortSupport structure pointer passed as the first argument, which will be configured with the appropriate comparator function

## Dependencies
- Functions called/Symbols referenced:
  - SortSupport (type)
  - ssup_datum_signed_cmp (on 64-bit builds)
  - [timestamp_fastcmp](timestamp_fastcmp.md) (on smaller builds)
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function uses conditional compilation (#if SIZEOF_DATUM >= 8) to optimize for different platform architectures
- This is part of PostgreSQL's sort support infrastructure, which allows data types to provide optimized comparison functions for sorting operations
- The choice between comparators is based on whether timestamps are pass-by-value (64-bit platforms) or pass-by-reference (32-bit platforms)