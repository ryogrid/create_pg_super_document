# PLyObject_ToComposite

## Location
[src/pl/plpython/plpy_typeio.c:941-1023](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L941-L1023)

## Overview
Converts a Python object to a PostgreSQL composite type (record/tuple), handling multiple input formats including sequences, mappings, and objects with __getattr__ methods.

## Definition


## Detailed Description
This function serves as the main entry point for converting Python objects to PostgreSQL composite types. It first handles the special case of None values by setting the isnull flag. For string inputs, it delegates to PLyUnicode_ToComposite for direct string-to-composite conversion.

The function manages tuple descriptor caching efficiently, distinguishing between named composite types (which require fresh lookups to detect schema changes) and RECORD types (which are stable). For named types, it validates the cached descriptor against the type's current definition and updates the cache if needed.

The conversion strategy depends on the Python object type:
- Sequences (tuples, lists) are processed by PLySequence_ToComposite
- Mappings (dictionaries) are handled by PLyMapping_ToComposite  
- Generic objects are converted via PLyGenericObject_ToComposite using __getattr__

The function ensures proper resource management by releasing tuple descriptors after conversion.

## Parameters / Member Variables
- : Conversion argument structure containing type information and cached conversion data
- : Python object to convert to PostgreSQL composite type
- : Output parameter set to true if the result should be NULL
- : Boolean indicating if this conversion is part of an array element conversion

## Dependencies
- Functions called/Symbols referenced:
  - [PLyUnicode_ToComposite](PLyUnicode_ToComposite.md)
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md)
  - [PLy_output_setup_tuple](PLy_output_setup_tuple.md)
  - [PLy_current_execution_context](PLy_current_execution_context.md)
  - PinTupleDesc
  - [PLySequence_ToComposite](PLySequence_ToComposite.md)
  - [PLyMapping_ToComposite](PLyMapping_ToComposite.md)
  - [PLyGenericObject_ToComposite](PLyGenericObject_ToComposite.md)
  - ReleaseTupleDesc
- Called from (representative examples):
  - [PLy_output_setup_tuple](PLy_output_setup_tuple.md)
  - [PLy_output_setup_func](PLy_output_setup_func.md)

## Notes and Other Information
The function implements an important optimization for RECORD types by caching tuple descriptors, since RECORD types cannot change between calls. For named composite types, it must always validate the descriptor to handle potential schema changes. The conversion routing based on Python object type (sequence vs mapping vs generic) provides flexible input handling while maintaining type safety.