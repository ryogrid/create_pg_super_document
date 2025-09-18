# PLyDict_FromTuple

## Location
[src/pl/plpython/plpy_typeio.c:815-878](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L815-L878)

## Overview
Converts a PostgreSQL HeapTuple into a Python dictionary by extracting each attribute and converting it to an appropriate Python object.

## Definition


## Detailed Description
This function performs the core work of converting a PostgreSQL tuple to a Python dictionary. It iterates through each attribute in the tuple descriptor, extracts the corresponding value from the tuple using heap_getattr, and converts each non-NULL value to a Python object using the appropriate conversion function. The function properly handles dropped attributes (skips them), generated columns (includes them only if requested), and NULL values (converts to Python None). The conversion process is wrapped in PostgreSQL's exception handling mechanism to ensure proper cleanup if errors occur during the conversion.

## Parameters / Member Variables
- : PLyDatumToOb structure containing conversion functions and metadata for each tuple attribute
- : HeapTuple containing the actual tuple data to be converted
- : TupleDesc describing the structure and metadata of the tuple
- : Boolean flag indicating whether generated columns should be included in the output dictionary

## Dependencies
- Functions called/Symbols referenced:
  - [PLyDatumToOb](PLyDatumToOb.md) (type structure and attribute array)
  - PG_TRY/PG_CATCH/PG_END_TRY (PostgreSQL exception handling macros)
  - [heap_getattr](../h/heap_getattr.md) (extracts attribute value from tuple)
  - PG_RE_THROW (re-throws caught exceptions)
- Called from:
  - [PLy_input_from_tuple](PLy_input_from_tuple.md) (main tuple input conversion entry point)
  - [PLyDict_FromComposite](PLyDict_FromComposite.md) (composite type conversion)

## Notes and Other Information
The function includes comprehensive error handling using PostgreSQL's exception system to ensure that partially constructed Python objects are properly cleaned up if an error occurs during conversion. It respects PostgreSQL's attribute metadata, properly skipping dropped attributes and handling generated columns according to the caller's preference. The function manages Python reference counting correctly, decrementing references for converted values after adding them to the dictionary. NULL values in the PostgreSQL tuple are represented as Python None objects in the resulting dictionary.