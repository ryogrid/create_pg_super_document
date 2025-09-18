# CoercionMethod

## Location
src/include/catalog/pg_cast.h: 92 - 106

## Overview
CoercionMethod is an enumeration that defines the allowable methods for type casting in PostgreSQL's pg_cast system catalog.

## Definition


## Detailed Description
The CoercionMethod enum specifies the three distinct approaches PostgreSQL uses to convert values from one data type to another. This enum is used as the domain for the castmethod column in the pg_cast system catalog table. The values are deliberately chosen as ASCII characters ('f', 'b', 'i') for human readability when examining the catalog directly, since the castmethod field is stored as a char type.

Each method represents a fundamentally different approach to type conversion:
- Function-based coercion relies on explicit cast functions
- Binary coercion leverages type compatibility at the storage level
- Input/output coercion uses the types' text representation as an intermediate format

## Parameters / Member Variables
-  ('f'): Indicates that casting should be performed using a specific cast function stored in the castfunc field of pg_cast
-  ('b'): Indicates that the source and target types are binary-compatible, requiring no conversion function
-  ('i'): Indicates that casting should be performed by converting the source value to text using its output function, then parsing with the target type's input function

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a simple enum definition)
- Called from (representative examples):
  - describe.c:4815-4816 (psql's \dC command)
  - pg_dump.c:12767-12773 (pg_dump cast handling)
  - functioncmds.c:1585-1587 (CREATE CAST command processing)

## Notes and Other Information
- This enum is part of the pg_cast system catalog infrastructure defined in src/include/catalog/pg_cast.h:87-92
- The ASCII character values enable easy inspection of cast methods in raw catalog data
- Binary coercion (COERCION_METHOD_BINARY) is the most efficient as it requires no actual conversion
- Input/output coercion (COERCION_METHOD_INOUT) is used when no direct cast function exists but both types have text representations
- Function coercion (COERCION_METHOD_FUNCTION) provides the most flexibility but requires explicit cast function implementation
- The enum is used within the EXPOSE_TO_CLIENT_CODE section, making it available to client applications that include PostgreSQL headers