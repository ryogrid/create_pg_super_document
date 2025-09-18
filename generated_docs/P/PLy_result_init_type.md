# PLy_result_init_type

## Location
src/pl/plpython/plpy_resultobject.c: 61 - 67

## Overview
Initializes the PLy_ResultType Python type object for the PLyResult class, making it ready for use in the PL/Python extension.

## Definition


## Detailed Description
This function performs the necessary initialization of the PLy_ResultType Python type object by calling PyType_Ready(). The PLy_ResultType defines the PLyResult class which represents the results of a PostgreSQL query in PL/Python. The function ensures that the type object is properly configured and ready to be used for creating PLyResult instances. If the initialization fails, it raises an ERROR using elog(), which will abort the current transaction.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - PyType_Ready() (Python C API)
  - elog() (PostgreSQL logging)
  - PLy_ResultType (static type object)
- Called from (representative examples):
  - [PLy_init_plpy](PLy_init_plpy.md)() at src/pl/plpython/plpy_plpymodule.c:153

## Notes and Other Information
- This function must be called during PL/Python initialization before any PLyResult objects can be created
- The PLy_ResultType defines a Python type that supports both sequence and mapping protocols
- Failure to initialize results in a PostgreSQL ERROR, which terminates the current transaction
- The PLy_ResultType includes methods like colnames(), coltypes(), nrows(), and status()
- This is part of the PL/Python extension's object model for representing query results