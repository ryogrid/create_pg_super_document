# PLy_subtransaction_new

## Location
src/pl/plpython/plpy_subxactobject.c: 54 - 70

## Overview
Creates and initializes a new PLy subtransaction object for use in Python code within PL/Python functions.

## Definition


## Detailed Description
This function creates a new PLySubtransactionObject instance that represents a subtransaction in the PLPython environment. It allocates memory for the object using PyObject_New() and initializes the object's state fields to their default values. The created subtransaction object can later be used to manage database subtransactions from Python code. This corresponds to the Python syntax `plpy.subtransaction()` within PL/Python functions.

## Parameters / Member Variables
- `self`: The module object (unused in this context)
- `unused`: Unused parameter for function signature compatibility

## Dependencies
- Functions called/Symbols referenced:
  - PyObject_New (Python C API function)
  - [PLySubtransactionObject](PLySubtransactionObject.md) (struct type)
  - PLy_SubtransactionType (Python type object)
- Called from:
  - No direct references found (likely registered as Python method)

## Notes and Other Information
- The function initializes both `started` and `exited` fields to false
- Returns NULL if memory allocation fails
- This is the constructor function for subtransaction objects in PL/Python
- The object must be properly entered and exited to function correctly
- Located in src/pl/plpython/plpy_subxactobject.c:54-70