# PLy_plan_new

## Location
[src/pl/plpython/plpy_planobject.c:48-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_planobject.c#L48-L65)

## Overview
Creates and initializes a new PLyPlan Python object that represents a prepared SQL statement in the PL/Python extension.

## Definition


## Detailed Description
This function allocates and initializes a new PLyPlanObject instance using the Python C API. The created object represents a prepared SQL plan that can be executed multiple times with different parameters. All member fields are initialized to safe default values (NULL or 0) to ensure the object is in a clean state. The function returns a PyObject pointer that can be used by Python code, or NULL if memory allocation fails.

## Parameters / Member Variables
This function takes no parameters and returns a PyObject pointer to the newly created PLyPlan instance.

## Dependencies
- Functions called/Symbols referenced:
  - PyObject_New (Python C API)
  - [PLyPlanObject](PLyPlanObject.md) (struct type)
  - PLy_PlanType (Python type object)
- Called from (representative examples):
  - [PLy_spi_prepare](PLy_spi_prepare.md)

## Notes and Other Information
- Returns NULL on memory allocation failure, which should be handled by calling code
- The created object has all fields initialized to safe default values:
  - plan: NULL (no prepared statement yet)
  - nargs: 0 (no arguments)
  - types, values, args: NULL (no parameter information)
  - mcxt: NULL (no memory context)
- The object must be properly populated after creation before it can be used for executing SQL
- Memory management follows Python reference counting semantics