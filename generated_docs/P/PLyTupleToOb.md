# PLyTupleToOb

## Location
[src/pl/plpython/plpy_typeio.h:40-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.h#L40-L50)

## Overview
PLyTupleToOb is a struct that contains conversion information for transforming PostgreSQL composite types (tuples) to Python dictionary objects.

## Definition

```c
typedef struct PLyTupleToOb
{
	/* If we're dealing with a RECORD type, actual descriptor is here: */
	TupleDesc	recdesc;
	/* If we're dealing with a named composite type, these fields are set: */
	TypeCacheEntry *typentry;	/* typcache entry for type */
	uint64		tupdescid;		/* last tupdesc identifier seen in typcache */
	/* These fields are NULL/0 if not yet set: */
	PLyDatumToOb *atts;			/* array of per-column conversion info */
	int			natts;			/* length of array */
} PLyTupleToOb;
```
## Detailed Description
PLyTupleToOb manages the conversion of PostgreSQL composite types and records to Python dictionaries. It handles both anonymous RECORD types and named composite types, caching conversion information for each column/attribute. The struct supports efficient conversion by maintaining type cache entries and column-specific conversion data.

## Parameters / Member Variables
- `recdesc`: TupleDesc for RECORD types containing the actual tuple descriptor
- `*typentry`: Type cache entry for named composite types providing type metadata
- `tupdescid`: Identifier for tracking changes in the type cache descriptor
- `*atts`: Array of PLyDatumToOb structures for converting each column/attribute
- `natts`: Number of attributes/columns in the tuple
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