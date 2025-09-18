# pgresAttDesc

## Location
src/interfaces/libpq/libpq-fe.h: 289 - 297

## Overview
The  struct represents metadata about a single column (attribute) in a PostgreSQL query result. It provides detailed information about column characteristics including name, data type, source table, and formatting options.

## Definition


## Detailed Description
The  structure contains comprehensive metadata about a column in a PostgreSQL query result set. It serves as a descriptor that provides both logical information (column name, source table/column) and physical characteristics (data type, size, format). This structure is fundamental to libpq's result handling, allowing client applications to understand the structure and data types of query results. The format field distinguishes between text and binary data representation, while the type information enables proper data interpretation and conversion.

## Parameters / Member Variables
- : The name of the column as it appears in the result set
- : OID of the source table if the column originates from a specific table, 0 if unknown
- : Attribute number of the column in the source table, 0 if unknown or not applicable
- : Format code indicating data representation (0 for text, 1 for binary)
- : PostgreSQL type OID that identifies the data type of the column
- : Size of the data type in bytes (-1 for variable-length types, -2 for null-terminated strings)
- : Type-specific modifier providing additional type information (e.g., precision for numeric types)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [Query](../Q/Query.md) result processing functions in libpq
  - PGresult structure members for column metadata

## Notes and Other Information
- This structure is part of the libpq public API for examining query result metadata
- The tableid and columnid fields help trace columns back to their source tables when possible
- Format codes support both text and binary result formats in PostgreSQL protocol
- Type modifiers (atttypmod) provide additional type constraints like precision, scale, or length limits
- [Variable](../V/Variable.md)-length types use typlen = -1, requiring actual length to be determined from the data
- The structure enables type-safe data extraction and conversion in client applications
- Binary format (format=1) provides more efficient data transfer for certain data types