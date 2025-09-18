# is_PLyPlanObject

## Location
[src/pl/plpython/plpy_planobject.c:66-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_planobject.c#L66-L71)

## Overview
A type-checking function that determines whether a given Python object is an instance of PLyPlan type.

## Definition


## Detailed Description
This utility function performs a type check to determine if the provided Python object is a PLyPlan instance. It works by comparing the object's type pointer (ob_type) against the address of the PLy_PlanType type object. This is a fast and reliable way to perform type checking in the Python C API, as it directly compares type pointers rather than performing string-based comparisons.

## Parameters / Member Variables
- : PyObject pointer to the Python object to be type-checked

## Dependencies
- Functions called/Symbols referenced:
  - PLy_PlanType (Python type object reference)
- Called from (representative examples):
  - [PLy_spi_execute](../P/PLy_spi_execute.md)

## Notes and Other Information
- Returns true if the object is a PLyPlan instance, false otherwise
- This is a lightweight operation that only performs a pointer comparison
- Used for type validation before casting PyObject to PLyPlanObject
- Essential for type safety when handling Python objects that may or may not be plan objects
- The function assumes the input PyObject pointer is valid (non-NULL)