# Int8GetDatum

## Location
[src/include/postgres.h:132-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L132-L141)

## Overview
Int8GetDatum is an inline function that converts an 8-bit integer (int8) value to PostgreSQL Datum representation.

## Definition
static inline Datum Int8GetDatum(int8 X)

## Detailed Description
Int8GetDatum provides type conversion from PostgreSQL int8 (8-bit signed integer) values to Datum format. The function performs a simple cast operation to store an 8-bit integer value within the Datum system. This enables consistent handling of small integer data throughout PostgreSQL operations, allowing int8 values to be stored, passed, and manipulated using the standard datum infrastructure. The function is part of PostgreSQLs comprehensive type conversion system that provides datum representations for all basic data types.

## Parameters / Member Variables
- `X`: An int8 (8-bit signed integer) value to be converted to Datum format for storage or manipulation within PostgreSQL systems.

## Dependencies
- Functions called/Symbols referenced:
  - int8 (type definition at line 132)
- Called from (representative examples):
  - Various PostgreSQL functions dealing with small integer values
  - SQL function implementations returning 8-bit integer results
  - Internal PostgreSQL operations handling int8 data types

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h for maximum performance
- Part of PostgreSQLs fundamental datum conversion system for type-safe value handling
- Provides consistent representation of 8-bit integers within the datum system
- Used for efficient storage and manipulation of small integer values
- Complements the broader family of datum conversion functions for different integer sizes
- The int8 type represents a signed 8-bit integer with range -128 to 127