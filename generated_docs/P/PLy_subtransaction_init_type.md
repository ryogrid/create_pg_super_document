# PLy_subtransaction_init_type

## Location
[src/pl/plpython/plpy_subxactobject.c:46-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_subxactobject.c#L46-L53)

## Overview
Initializes the Python type object for PLy_SubtransactionType, preparing it for use in the PLPython extension's subtransaction management.

## Definition


## Detailed Description
This function initializes the PLy_SubtransactionType Python type object by calling PyType_Ready(). It is part of the PLPython extension's initialization sequence and ensures that the subtransaction type is properly set up for Python to use. The function is essential for enabling subtransaction functionality in PL/Python stored procedures and functions. If the type initialization fails, it raises an ERROR using PostgreSQL's elog mechanism.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - PyType_Ready (Python C API function)
  - elog (PostgreSQL logging function)
  - PLy_SubtransactionType (static type definition)
- Called from:
  - [PLy_init_plpy](PLy_init_plpy.md) (at src/pl/plpython/plpy_plpymodule.c:154)

## Notes and Other Information
- This function must be called during PLPython module initialization before any subtransaction objects can be created
- Failure to initialize the type results in a PostgreSQL ERROR, preventing the PLPython extension from loading
- The function is part of the broader PLPython type system initialization
- Located in src/pl/plpython/plpy_subxactobject.c:46-53