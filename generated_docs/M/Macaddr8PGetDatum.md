# Macaddr8PGetDatum

## Location
[src/include/utils/inet.h:169-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/inet.h#L169-L173)

## Overview
A static inline function that converts a macaddr8 pointer to a PostgreSQL Datum value, used for returning MAC address data through PostgreSQL's function call interface.

## Definition
static inline Datum
Macaddr8PGetDatum(const macaddr8 *X)

## Detailed Description
Macaddr8PGetDatum is a type conversion utility function that wraps a macaddr8 pointer into a PostgreSQL Datum. This function is part of PostgreSQL's type system infrastructure for handling MAC8 addresses (8-byte MAC addresses). It performs a simple pointer conversion operation, converting a specific macaddr8 pointer type to the generic Datum type used throughout PostgreSQL's internal APIs. Since macaddr8 is a fixed-length pass-by-reference datatype, this function essentially wraps the pointer for storage in a Datum.

## Parameters / Member Variables
- X: A const pointer to a macaddr8 structure containing the 8-byte MAC address data

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - [macaddr8](../m/macaddr8.md) (struct type)
- Called from (representative examples):
  - PG_RETURN_MACADDR8_P

## Notes and Other Information
- This is a static inline function defined in src/include/utils/inet.h:169-173
- Part of PostgreSQL's type conversion infrastructure for MAC8 addresses
- The macaddr8 type represents 8-byte MAC addresses with structure members a through h
- Used in conjunction with PostgreSQL's function call interface macros
- Takes a const pointer parameter indicating the macaddr8 data should not be modified
- Returns a Datum value containing the pointer for use in PostgreSQL's internal systems