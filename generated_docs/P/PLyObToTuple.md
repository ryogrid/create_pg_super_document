# PLyObToTuple

## Location
src/pl/plpython/plpy_typeio.h: 105 - 117

## Overview
PLyObToTuple is a specialized conversion structure used within PostgreSQL's PLpython extension to handle conversion of Python objects to PostgreSQL composite/record data types.

## Definition
```c
typedef struct PLyObToTuple
{
    /* If we're dealing with a RECORD type, actual descriptor is here: */
    TupleDesc       recdesc;
    /* If we're dealing with a named composite type, these fields are set: */
    TypeCacheEntry *typentry;       /* typcache entry for type */
    uint64          tupdescid;      /* last tupdesc identifier seen in typcache */
    /* These fields are NULL/0 if not yet set: */
    PLyObToDatum   *atts;           /* array of per-column conversion info */
    int             natts;          /* length of array */
    /* We might need to convert using record_in(); if so, cache info here */
    FmgrInfo        recinfunc;      /* lookup info for record_in */
} PLyObToTuple;
```

## Detailed Description
PLyObToTuple is a component structure used as part of the PLyObToDatum conversion system for handling PostgreSQL composite types (tuples/records). It manages the complex conversion of Python objects (dictionaries, sequences, or generic objects) into PostgreSQL composite values. The structure handles both anonymous RECORD types and named composite types, with different fields used depending on the scenario. It maintains per-column conversion information and caches tuple descriptor information for efficient repeated conversions.

## Parameters / Member Variables
- `recdesc`: TupleDesc containing the actual tuple descriptor when dealing with anonymous RECORD types
- `typentry`: TypeCacheEntry pointer for named composite types, providing cached type information
- `tupdescid`: Identifier for the last tuple descriptor seen in the type cache, used for cache validation
- `atts`: Array of PLyObToDatum structures containing conversion information for each column/attribute
- `natts`: Number of attributes/columns in the tuple (length of the atts array)
- `recinfunc`: FmgrInfo structure containing cached lookup information for the record_in function when string-based conversion is needed

## Dependencies
- Functions called/Symbols referenced:
  - [PLyObToDatum](PLyObToDatum.md) (for per-column conversion)
  - [TupleDesc](../T/TupleDesc.md) (PostgreSQL tuple descriptor)
  - [TypeCacheEntry](../T/TypeCacheEntry.md) (PostgreSQL type cache entry)
  - [FmgrInfo](../F/FmgrInfo.md) (PostgreSQL function manager structure)
- Called from (representative examples):
  - [PLyObToDatum](PLyObToDatum.md) (as union member 'tuple')
  - Composite type conversion functions in plpy_typeio.c
  - [PLyMapping_ToComposite](PLyMapping_ToComposite.md), PLySequence_ToComposite, PLyGenericObject_ToComposite

## Notes and Other Information
PLyObToTuple handles the most complex conversions in PLpython by supporting multiple Python object representations (mappings/dicts, sequences/tuples, and generic objects with attributes) and converting them to PostgreSQL composite types. The structure optimizes for both anonymous RECORD types (using recdesc) and named composite types (using typentry and tupdescid for cache validation). The atts array enables per-column type-specific conversion, allowing composite types with heterogeneous column types. The recinfunc provides a fallback mechanism for string-based record conversion when direct conversion is not possible. This structure is essential for PLpython's ability to work with complex PostgreSQL data types and return composite values from Python functions.