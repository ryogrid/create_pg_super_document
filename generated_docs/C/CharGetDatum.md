# CharGetDatum

## Location
src/include/postgres.h: 122 - 131

## Overview
CharGetDatum is an inline function that converts a C char value to PostgreSQL Datum representation.

## Definition
static inline Datum CharGetDatum(char X)

## Detailed Description
CharGetDatum provides the complement to DatumGetChar, converting C char values to PostgreSQL Datum format. The function performs a simple cast operation to store a character value within the Datum system. This enables consistent handling of character data throughout PostgreSQL operations, allowing char values to be stored, passed, and manipulated using the standard datum infrastructure. The function is widely used throughout PostgreSQL for catalog operations, type system functions, and SQL function implementations that work with character data.

## Parameters / Member Variables
- `X`: A C char value to be converted to Datum format for storage or manipulation within PostgreSQL systems.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple type cast)
- Called from (representative examples):
  - [objectsInSchemaToOids](../o/objectsInSchemaToOids.md) (src/backend/catalog/aclchk.c:901, 906)
  - [InsertPgAttributeTuples](../I/InsertPgAttributeTuples.md) (src/backend/catalog/heap.c:748-755)
  - [AggregateCreate](../A/AggregateCreate.md) (src/backend/catalog/pg_aggregate.c:660, 672-673)
  - [ProcedureCreate](../P/ProcedureCreate.md) (src/backend/catalog/pg_proc.c:308, 313-314)
  - [TypeCreate](../T/TypeCreate.md) (src/backend/catalog/pg_type.c:358-375)
  - [CreatePolicy](CreatePolicy.md) (src/backend/commands/policy.c:695)
  - PG_RETURN_CHAR (src/include/fmgr.h:358)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h for maximum performance
- Extensively used throughout PostgreSQL catalog management and type system operations
- Forms a bidirectional conversion pair with DatumGetChar for char/Datum interoperability
- Essential for SQL function implementations that return char values
- Part of the fundamental datum conversion system enabling type-safe character value handling
- Used heavily in catalog tuple creation and system metadata management