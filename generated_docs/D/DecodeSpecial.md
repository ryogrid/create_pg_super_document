# DecodeSpecial

## Location
src/interfaces/ecpg/pgtypeslib/dt_common.c: 635 - 668

## Overview
A cached lookup function that decodes special date/time keywords and tokens using the main date token lookup table, optimized for performance through caching.

## Definition


## Detailed Description
DecodeSpecial recognizes special keywords listed in the datetktbl (date token table) through an optimized lookup mechanism. The function was historically used for both special date keywords and timezone abbreviations, but timezone abbreviation recognition has been moved to DecodeTimezoneAbbrev(). 

Like DecodeUnits, this function implements a cache mechanism (datecache) to improve performance based on the assumption that date formats will often be related or repeated. The function first checks if there's a cached entry for the given field that matches the input token. If no cache hit occurs, it performs a binary search using datebsearch() on the main datetktbl. The input string must be pre-lowercased before calling this function.

## Parameters / Member Variables
- : Index for the cache array to store/retrieve cached tokens
- : The lowercased string token representing a special date/time keyword to decode
- : Output parameter that receives the decoded numeric value associated with the token

## Dependencies
- Functions called/Symbols referenced:
  - datetkn (structure type for date/time tokens)
  - TOKMAXLEN (maximum token length constant)
  - datebsearch (binary search function for date tokens)
  - UNKNOWN_FIELD (constant returned when token is not recognized)
  - datecache (cache array for storing recent lookups)
  - datetktbl (main lookup table for date/time tokens)
  - szdatetktbl (size of the datetktbl array)
- Called from (representative examples):
  - DecodeDateTime
  - DecodeDate
  - DecodeTimeOnly
  - extract_date
  - timestamp_part_common
  - DecodeInterval

## Notes and Other Information
- Returns the type of the decoded token (from the datetkn structure) or UNKNOWN_FIELD if not found
- The caching mechanism significantly improves performance for repetitive date/time parsing operations
- Input token must be pre-lowercased - the function does not perform case conversion
- Uses strncmp with TOKMAXLEN to support truncated token matching
- The val parameter is set to 0 if the token is not recognized
- Central to parsing special date/time keywords like 'today', 'tomorrow', 'yesterday', epoch values, and other special date constants
- Works alongside DecodeUnits but focuses on the main date token table rather than interval units
- Essential for PostgreSQL's flexible date/time input parsing that supports various keyword formats