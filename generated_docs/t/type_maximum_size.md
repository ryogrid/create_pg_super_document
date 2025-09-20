# type_maximum_size

## Location
[src/backend/utils/adt/format_type.c:412-446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/format_type.c#L412-L446)

## Overview
Determines the maximum possible width (in bytes) of a variable-width PostgreSQL data type column given its type OID and type modifier.

## Definition

```c
int32
type_maximum_size(Oid type_oid, int32 typemod)
```
## Detailed Description
The  function calculates the maximum storage size that a variable-width data type can occupy, taking into account the type modifier constraints. This function is crucial for storage planning, memory allocation, and optimization decisions in PostgreSQL.

The function handles several specific data types with known size calculation methods:
- **BPCHAR/VARCHAR**: Calculates size based on character length, considering database encoding and including varlena header overhead
- **NUMERIC**: Delegates to  for precision-based calculation  
- **VARBIT/BIT**: Calculates size based on bit count, converting to bytes with proper alignment

For unknown types or types with unlimited width (like 'text'), the function returns -1 to indicate indeterminate size. The caller is assumed to have already verified that the type is variable-width.

## Parameters / Member Variables
- : The PostgreSQL type OID identifying the specific data type
- : The type modifier value that constrains the type (e.g., length for varchar, precision for numeric)

## Dependencies
- Functions called/Symbols referenced:
  - : Gets the current database's character encoding
  - : Returns maximum bytes per character for the encoding
  - : Calculates maximum size for NUMERIC types
  - : Constant for bit-to-byte conversion
- Called from (representative examples):
  - : Used to determine if TOAST storage is needed
  - : Used in statistics calculations for query planning

## Notes and Other Information
- Returns -1 for indeterminate or unlimited-width types, or when typemod < 0
- For character types (BPCHAR/VARCHAR), the calculation includes the varlena header (VARHDRSZ) and considers multi-byte character encodings
- For bit types, the function performs ceiling division to convert bits to bytes and adds overhead for length storage
- The function shares type modifier encoding knowledge with , suggesting potential for code refactoring
- Critical for PostgreSQL's storage management and query optimization systems
- Used in determining whether tables need TOAST (The Oversized-Attribute Storage Technique) for large values