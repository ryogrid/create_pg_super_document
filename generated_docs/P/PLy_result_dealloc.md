# PLy_result_dealloc

## Location
src/pl/plpython/plpy_resultobject.c: 92 - 108

## Overview
Deallocates a PLyResultObject instance, properly cleaning up all its contained Python objects and PostgreSQL resources.

## Definition
```c
static void PLy_result_dealloc(PyObject *arg)
```

## Detailed Description
This function serves as the destructor for PLyResultObject instances in the Python type system. It is automatically called by Python's garbage collector when the reference count of a PLyResult object reaches zero. The function carefully deallocates all contained objects and resources to prevent memory leaks, including Python objects (using Py_XDECREF) and PostgreSQL-specific resources like tuple descriptors (using FreeTupleDesc). After cleaning up all contained resources, it calls the type's tp_free function to deallocate the object itself.

## Parameters / Member Variables
- `arg`: PyObject pointer to the PLyResultObject being deallocated

## Dependencies
- Functions called/Symbols referenced:
  - Py_XDECREF() (Python C API - safe decrement reference)
  - FreeTupleDesc() (PostgreSQL tuple descriptor deallocation)
  - tp_free() (Python type's free function)
  - PLyResultObject (struct type)
- Called from (representative examples):
  - Referenced as tp_dealloc in PLy_ResultType at src/pl/plpython/plpy_resultobject.c:51
  - Automatically invoked by Python's garbage collector

## Notes and Other Information
- This is a static function that serves as a callback for Python's type system
- Uses Py_XDECREF instead of Py_DECREF for safe handling of potentially NULL Python objects
- Properly handles the PostgreSQL TupleDesc resource by calling FreeTupleDesc and setting to NULL
- Follows the standard Python deallocation pattern: clean up contained objects first, then free the object itself
- The function is registered in the PLy_ResultType definition and called automatically by Python
- Critical for preventing memory leaks in the PL/Python extension
- Must not be called directly by user code - it's managed by Python's reference counting system