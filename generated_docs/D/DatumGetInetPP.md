# DatumGetInetPP

## Location
[src/include/utils/inet.h:123-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/inet.h#L123-L128)

## Overview
Converts a Datum value to a pointer to an inet structure, handling detoasting of packed data for network address operations.

## Definition

```c
static inline inet *
DatumGetInetPP(Datum X)
```
## Detailed Description
DatumGetInetPP is an inline function that extracts an inet pointer from a Datum value. It specifically handles packed/compressed datum values by calling PG_DETOAST_DATUM_PACKED, which efficiently detoasts the data without creating a full copy if the data is already unpacked. This function is part of PostgreSQL's fmgr (function manager) interface macros for the inet data type, providing efficient access to network address data.

The "PP" suffix indicates this function returns a pointer that may point to packed data, which should be treated as read-only to avoid corruption.

## Parameters / Member Variables
- `X`: A Datum value containing a packed or unpacked inet structure

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_PACKED
  - inet
- Called from (representative examples):
  - [network_fast_cmp](../n/network_fast_cmp.md)
  - [network_abbrev_convert](../n/network_abbrev_convert.md)
  - [convert_network_to_scalar](../c/convert_network_to_scalar.md)
  - [inet_gist_compress](../i/inet_gist_compress.md)
  - [inet_hist_value_sel](../i/inet_hist_value_sel.md)
  - [inet_spg_choose](../i/inet_spg_choose.md)
  - [inet_spg_picksplit](../i/inet_spg_picksplit.md)
  - [inet_spg_inner_consistent](../i/inet_spg_inner_consistent.md)
  - [inet_spg_leaf_consistent](../i/inet_spg_leaf_consistent.md)
  - [inet_spg_consistent_bitmap](../i/inet_spg_consistent_bitmap.md)
  - PG_GETARG_INET_PP (macro)

## Notes and Other Information
- This is an inline function defined in src/include/utils/inet.h for performance
- The returned pointer may point to packed data and should be treated as read-only
- Used extensively in network address indexing (GiST, SP-GiST), comparison operations, and statistical analysis
- Part of the PostgreSQL function manager interface for type conversion
- Preferred over DatumGetInetP when the data will only be read, not modified

## Simplified Source

```c
static inline inet * DatumGetInetPP(Datum X) {
    // Extract inet pointer from Datum, handling packed data efficiently
    return (inet *) PG_DETOAST_DATUM_PACKED(X);
}
```