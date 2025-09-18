# DefineAttr

## Location
[src/backend/bootstrap/bootstrap.c:490-597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L490-L597)

## Overview
DefineAttr defines an attribute (column) for a relation during the bootstrap process, setting up the attribute metadata including type information, nullability, and storage characteristics.

## Definition
```c
void DefineAttr(char *name, char *type, int attnum, int nullness)
```

## Detailed Description
DefineAttr is a core bootstrap function that defines individual attributes (columns) for relations being created during PostgreSQL initialization. This function is called once for each column in a relation to be created, building up the attribute metadata in the global `attrtypes` array.

The function performs several key operations:
1. Validates that no relations are currently open for bootstrap commands
2. Allocates and initializes an attribute structure if needed
3. Sets the attribute name and number
4. Resolves the type information using either the Typ list or TypInfo array
5. Configures storage characteristics (length, alignment, storage type)
6. Forces system catalog columns to use C collation for consistency
7. Determines nullability based on the provided parameter and column positioning

For system catalogs, the function ensures collation-aware columns use C collation to maintain consistency across different database collations, which is essential for template0 cloning.

## Parameters / Member Variables
- `name`: The name of the attribute (column) to be defined
- `type`: String representation of the data type for this attribute
- `attnum`: Zero-based attribute number (position) within the relation
- `nullness`: Nullability constraint, can be BOOTCOL_NULL_FORCE_NOT_NULL, BOOTCOL_NULL_FORCE_NULL, or BOOTCOL_NULL_AUTO

## Dependencies
- Functions called/Symbols referenced:
  - [closerel](../c/closerel.md) (closes any open relations)
  - [AllocateAttribute](../A/AllocateAttribute.md) (allocates attribute structure)
  - MemSet (clears attribute structure)
  - namestrcpy (copies attribute name)
  - [gettype](../g/gettype.md) (resolves type information)
  - ATTRIBUTE_FIXED_PART_SIZE (constant for structure size)
  - InvalidCompressionMethod (default compression setting)
  - BOOTCOL_NULL_* constants (nullability options)
- Called from (representative examples):
  - Bootstrap parser during system initialization

## Notes and Other Information
- This function is part of the bootstrap process and only used during PostgreSQL system initialization
- The function maintains a global attrtypes array that accumulates attribute definitions
- For array types with variable length, the function assumes 1-dimensional arrays
- System catalog columns are forced to use C collation (C_COLLATION_OID) for database-independent behavior
- The function implements automatic nullability detection for fixed-width columns when BOOTCOL_NULL_AUTO is specified
- Warning is issued if relations are left open when this command is executed