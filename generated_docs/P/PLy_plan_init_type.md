# PLy_plan_init_type

## Location
[src/pl/plpython/plpy_planobject.c:41-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_planobject.c#L41-L47)

## Overview
Initializes the PLyPlan Python type object for use in the PL/Python extension, making it ready for instantiation and use within the Python interpreter.

## Definition


## Detailed Description
This function performs the necessary initialization of the PLy_PlanType Python type object by calling PyType_Ready(). The PLy_PlanType represents prepared SQL statements (plans) that can be executed multiple times with different parameters in PL/Python. The function ensures that the type is properly registered with the Python interpreter and ready for object creation. If initialization fails, it raises a PostgreSQL ERROR using elog().

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - PyType_Ready (Python C API)
  - elog (PostgreSQL logging)
  - PLy_PlanType (static type object)
- Called from (representative examples):
  - [PLy_init_plpy](PLy_init_plpy.md)

## Notes and Other Information
- This function must be called during PL/Python extension initialization before any PLyPlan objects can be created
- The PLy_PlanType defines the Python class "PLyPlan" with methods like cursor(), execute(), and status()
- Failure to initialize results in a PostgreSQL ERROR, preventing the extension from loading
- The function is part of the PL/Python infrastructure for executing prepared SQL statements from Python code