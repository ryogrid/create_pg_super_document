# TupleDescInitEntry

## Location
src/backend/access/common/tupdesc.c: 651 - 725

## Overview
TupleDescInitEntry initializes a single attribute structure within a previously allocated TupleDesc, setting up all necessary field values by looking up type information from the system catalog.

## Definition


## Detailed Description
This function performs comprehensive initialization of a single attribute slot in a TupleDesc structure. It combines caller-provided information with type metadata retrieved from the system catalog to fully populate the pg_attribute structure.

The initialization process includes:
1. **Basic attribute setup**: Sets attribute number, name (if provided), type modifier, and array dimensions
2. **Type information retrieval**: Performs a system catalog lookup to get detailed type information including length, alignment, storage mode, and collation
3. **Default value initialization**: Sets reasonable defaults for constraint-related fields like nullability, defaults, and inheritance properties
4. **Storage attribute setup**: Configures physical storage characteristics based on the type definition

The function handles several special cases:
- NULL attribute names are replaced with empty strings
- Existing attribute names can be preserved by passing the current name
- Array dimensions are validated against system limits
- Default collation is set based on the type's collation property

## Parameters / Member Variables
- : The TupleDesc structure containing the attribute to initialize
- : 1-based attribute position within the TupleDesc (must be valid)
- : Name for the attribute (NULL creates empty name, existing name preserves current name)
- : PostgreSQL type OID identifying the data type
- : Type modifier value for the attribute
- : Number of array dimensions (0 for non-arrays, validated against PG_INT16_MAX)

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_type (type catalog structure access)
  - PointerIsValid (validation macro)
  - PG_INT16_MAX (array dimension limit)
  - MemSet (memory clearing)
  - NAMEDATALEN (name length constant)
  - namestrcpy (name copying utility)
  - InvalidCompressionMethod (compression default)
- Called from (representative examples):
  - [BuildDescFromLists](../B/BuildDescFromLists.md) (tuple descriptor construction)
  - [ExecTypeFromTLInternal](../E/ExecTypeFromTLInternal.md) (executor type setup)
  - [create_toast_table](../c/create_toast_table.md) (TOAST table creation)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md) (parser function handling)

## Notes and Other Information
- Performs system catalog lookup via SearchSysCache1/ReleaseSysCache for type metadata
- Sets attcollation to the default collation for the specified data type
- For non-default collations, use TupleDescInitEntryCollation afterwards
- Initializes physical storage fields (attlen, attbyval, attalign, attstorage) from type definition
- Sets conservative defaults for constraint fields (attnotnull=false, atthasdef=false, etc.)
- Widely used throughout PostgreSQL for dynamic tuple descriptor construction
- Critical component in function result type determination and table creation processes