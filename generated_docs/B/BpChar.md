# BpChar

## Location
src/include/c.h: 701 - 701

## Overview
The  type implements PostgreSQL's SQL  data type, providing fixed-length, blank-padded character strings with a specified maximum length.

## Definition


## Detailed Description
The  type ("Blank-Padded Character") is PostgreSQL's implementation of the SQL standard  data type. It stores fixed-length character strings that are automatically padded with trailing spaces to reach the specified length. Despite being "fixed-length" logically, it uses the  structure for storage efficiency, as trailing spaces are typically not stored physically.

Key characteristics:
- **Fixed Length**: Logically maintains a fixed length specified by a type modifier
- **Blank Padding**: Automatically pads strings with trailing spaces to reach the specified length
- **Space Trimming**: Trailing spaces are often optimized away in storage but maintained for comparison semantics
- **SQL Standard Compliance**: Follows SQL standard behavior for CHAR(n) types
- **UTF-8 Encoding**: Supports full Unicode character sets
- **Type Modifier**: Requires a length specification (e.g., CHAR(10))

The type provides SQL standard semantics where shorter strings are conceptually padded with spaces for comparison purposes.

## Parameters / Member Variables
As a typedef of ,  inherits:
- : Length and metadata field (do not access directly)
- : UTF-8 encoded character content

## Dependencies
- Functions called/Symbols referenced:
  - varlena (base structure)
- Called from (representative examples):
  - bpcharin (input function)
  - bpchar (type conversion)
  - bpchareq, bpcharne (comparison functions)
  - bpcharlt, bpcharle, bpchargt, bpcharge (ordering functions)
  - bpcharcmp (comparison function)
  - hashbpchar (hash function)
  - bcTruelen (length calculation)
  - bpchar_pattern_* (pattern matching functions)

## Notes and Other Information
- **SQL CHAR(n) Semantics**: Implements exact SQL standard behavior for fixed-length character types
- **Storage Optimization**: Trailing spaces are typically not stored physically to save space
- **Comparison Behavior**: Comparison treats logical trailing spaces as present even when not stored
- **Type Modifier Required**: Cannot be used without specifying a length (e.g., CHAR(20))
- **Blank Padding**: Shorter input strings are logically extended with spaces
- **Truncation**: Input strings longer than the specified length are truncated (with warnings)
- **Pattern Matching**: Supports both regular and pattern-based comparison operations
- **Hashing**: Provides specialized hash functions for use in hash indexes and joins
- **Collation Support**: Respects collation rules for comparison and sorting operations
- **Legacy Usage**: Less commonly used in modern applications compared to VARCHAR or TEXT