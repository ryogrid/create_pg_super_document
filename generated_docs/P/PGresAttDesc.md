# PGresAttDesc

## Location
[src/interfaces/libpq/libpq-fe.h:298-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-fe.h#L298-L322)

## Overview
PGresAttDesc is a structure that describes a single column (attribute) in a PostgreSQL query result, containing metadata about the column's name, source, data type, and formatting information.

## Definition


## Detailed Description
PGresAttDesc is a core data structure in PostgreSQL's libpq client library that provides comprehensive metadata about individual columns in query results. This structure is essential for applications that need to understand the schema and characteristics of result sets before processing the actual data. It contains both logical information (like column names and types) and physical information (like formatting and storage details).

The structure serves as the foundation for result set introspection, enabling client applications to dynamically handle different types of queries and result formats. It supports both text and binary result formats, provides type information for proper data conversion, and includes source table information for applications that need to trace data back to its origin.

## Parameters / Member Variables
- : The name of the column as it appears in the result set (may be aliased in the query)
- : Object identifier (OID) of the source table from which this column originates, or 0 if unknown/not applicable
- : The column number within the source table, or 0 if unknown/not applicable (1-based indexing)
- : Format code indicating how values are encoded - 0 for text format, 1 for binary format
- : PostgreSQL type OID that identifies the data type of this column (e.g., INT4OID, TEXTOID)
- : Size of the data type in bytes, or -1 for variable-length types, -2 for null-terminated strings
- : Type-specific modifier providing additional type information (e.g., precision for numeric types, length for varchar)

## Dependencies
- Functions called/Symbols referenced:
  - [PQconnectStart](PQconnectStart.md)
  - [PQconnectStartParams](PQconnectStartParams.md)
  - [PQconnectPoll](PQconnectPoll.md)
  - PostgresPollingStatusType
  - [PQconnectdb](PQconnectdb.md)
  - [PQconnectdbParams](PQconnectdbParams.md)
  - PQsetdbLogin
- Called from (representative examples):
  - [PQsetResultAttrs](PQsetResultAttrs.md)
  - [getRowDescriptions](../g/getRowDescriptions.md)
  - [getCopyStart](../g/getCopyStart.md)
  - [pg_result](../p/pg_result.md) (as part of result structure)

## Notes and Other Information
- This structure is used internally by libpq to store column metadata received from the PostgreSQL server
- Arrays of PGresAttDesc structures describe the complete schema of a result set
- The format field enables applications to handle both text and binary result protocols
- Type information (typid, typlen, atttypmod) allows for proper data type handling and conversion
- Source table information (tableid, columnid) enables applications to trace columns back to their database origins
- Used extensively in result processing functions like PQfname(), PQftype(), PQfmod(), etc.
- Essential for applications that need schema introspection capabilities
- The atttypmod field interpretation depends on the specific data type (e.g., for numeric types, it encodes precision and scale)