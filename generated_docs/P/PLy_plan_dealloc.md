# PLy_plan_dealloc

## Location
[src/pl/plpython/plpy_planobject.c:72-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_planobject.c#L72-L90)

## Overview
The destructor function for PLyPlan Python objects that performs proper cleanup of PostgreSQL resources when the object is garbage collected.

## Definition

```c
static void
PLy_plan_dealloc(PyObject *arg)
```
## Detailed Description
This function serves as the tp_dealloc callback for the PLy_PlanType Python type, automatically called by the Python interpreter during garbage collection of PLyPlan objects. It performs essential cleanup by freeing the associated PostgreSQL prepared plan using SPI_freeplan() and deleting any associated memory context. After cleaning up PostgreSQL-specific resources, it delegates to the standard Python object deallocation mechanism to free the Python object itself.

## Parameters / Member Variables
- `*arg`: PyObject pointer to the PLyPlan object being deallocated (cast to PLyPlanObject internally)
## Dependencies
- Functions called/Symbols referenced:
  - [PLyPlanObject](PLyPlanObject.md) (struct type cast)
  - [SPI_freeplan](../S/SPI_freeplan.md) (PostgreSQL SPI function)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (PostgreSQL memory management)
  - tp_free (Python object deallocation)
- Called from (representative examples):
  - Python garbage collector (via tp_dealloc callback)

## Notes and Other Information
- This function is registered as the tp_dealloc callback in PLy_PlanType
- Essential for preventing memory leaks of PostgreSQL prepared plans
- Safely handles cases where plan or mcxt are NULL (no cleanup needed)
- The function is static and only called internally by the Python interpreter
- Proper cleanup is critical since PostgreSQL resources are not automatically managed by Python's garbage collection
- Sets freed pointers to NULL to prevent double-free errors