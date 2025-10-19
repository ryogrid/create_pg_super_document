# InetPGetDatum

## Location
[src/include/utils/inet.h:129-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/inet.h#L129-L133)

## Overview
Converts an inet pointer to a Datum value for use in PostgreSQL's function manager interface.

## Definition
```c
static inline Datum
InetPGetDatum(const inet *X)
```

## Detailed Description
InetPGetDatum is an inline function that converts a pointer to an inet structure into a Datum value. It simply wraps the PointerGetDatum function, providing a type-safe interface specifically for inet data types. This function is part of PostgreSQL's fmgr (function manager) interface macros for the inet data type, enabling inet pointers to be passed as return values or arguments in the PostgreSQL function call interface.

## Parameters / Member Variables
- `X`: A constant pointer to an inet structure to be converted to Datum

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - inet
- Called from (representative examples):
  - [inet_gist_fetch](../i/inet_gist_fetch.md)
  - [inet_spg_choose](../i/inet_spg_choose.md)
  - [inet_spg_picksplit](../i/inet_spg_picksplit.md)
  - [inet_spg_leaf_consistent](../i/inet_spg_leaf_consistent.md)
  - PG_RETURN_INET_P (macro)

## Notes and Other Information
- This is an inline function defined in src/include/utils/inet.h for performance
- Takes a const pointer parameter, indicating the inet data should not be modified through this interface
- Primarily used in indexing operations (GiST, SP-GiST) for network addresses
- Part of the PostgreSQL function manager interface for type conversion
- The PG_RETURN_INET_P macro provides a convenient wrapper for function return statements

## Simplified Source

```c
static inline Datum
InetPGetDatum(const inet *X)
{
    // Convert inet pointer to generic Datum value
    return PointerGetDatum(X);
}
```