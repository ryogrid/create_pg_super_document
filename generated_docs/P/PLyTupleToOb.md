# PLyTupleToOb

## Location
src/pl/plpython/plpy_typeio.h: 40 - 50

## Overview
PLyTupleToOb is a struct that contains conversion information for transforming PostgreSQL composite types (tuples) to Python dictionary objects.

## Definition


## Detailed Description
PLyTupleToOb manages the conversion of PostgreSQL composite types and records to Python dictionaries. It handles both anonymous RECORD types and named composite types, caching conversion information for each column/attribute. The struct supports efficient conversion by maintaining type cache entries and column-specific conversion data.

## Parameters / Member Variables
- : TupleDesc for RECORD types containing the actual tuple descriptor
- : Type cache entry for named composite types providing type metadata
- : Identifier for tracking changes in the type cache descriptor
- : Array of PLyDatumToOb structures for converting each column/attribute
- : Number of attributes/columns in the tuple

## Dependencies
- Functions called/Symbols referenced:
  - [TupleDesc](../T/TupleDesc.md) (PostgreSQL tuple descriptor)
  - [TypeCacheEntry](../T/TypeCacheEntry.md) (PostgreSQL type cache)
  - [PLyDatumToOb](PLyDatumToOb.md) (for attribute conversions)
- Called from (representative examples):
  - [PLyDatumToOb](PLyDatumToOb.md) (as part of the union)
  - [PLy_input_setup_tuple](PLy_input_setup_tuple.md)
  - [PLyDict_FromComposite](PLyDict_FromComposite.md)

## Notes and Other Information
This struct efficiently handles both row types and user-defined composite types by caching conversion information for each attribute. It supports dynamic type changes by tracking tuple descriptor identifiers and rebuilding conversion info when necessary.