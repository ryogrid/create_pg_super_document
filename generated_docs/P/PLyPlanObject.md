# PLyPlanObject

## Location
src/pl/plpython/plpy_planobject.h: 12 - 21

## Overview
PLyPlanObject is a C structure that represents a PostgreSQL execution plan within the PL/Python language extension, serving as a Python object wrapper for prepared SQL statements that can be executed multiple times with different parameter values.

## Definition
```c
typedef struct PLyPlanObject
{
	PyObject_HEAD
	SPIPlanPtr	plan;
	int			nargs;
	Oid		   *types;
	Datum	   *values;
	PLyObToDatum *args;
	MemoryContext mcxt;
} PLyPlanObject;
```

## Detailed Description
PLyPlanObject is the core data structure used by PL/Python to represent prepared SQL statements. It bridges PostgreSQL's Server Programming Interface (SPI) with Python objects, allowing Python functions to prepare SQL statements once and execute them multiple times with different parameters. The structure contains all necessary information for parameter binding, type conversion, and memory management. It inherits from PyObject_HEAD, making it a proper Python object that can be manipulated from Python code through methods like execute(), cursor(), and status().

## Parameters / Member Variables
- `PyObject_HEAD`: Standard Python object header that makes this structure a Python object
- `plan`: SPIPlanPtr pointing to the actual PostgreSQL execution plan created by SPI_prepare
- `nargs`: Integer count of the number of arguments/parameters this plan expects
- `types`: Array of Oid values representing the PostgreSQL data types of each parameter
- `values`: Array of Datum values holding the actual parameter values for execution
- `args`: Array of PLyObToDatum converters for transforming Python objects to PostgreSQL Datum values
- `mcxt`: MemoryContext used for managing memory allocation related to this plan object

## Dependencies
- Functions called/Symbols referenced:
  - [SPIPlanPtr](../S/SPIPlanPtr.md) (from executor/spi.h)
  - [PLyObToDatum](PLyObToDatum.md) (from plpy_typeio.h)
- Called from (representative examples):
  - [PLy_plan_new](PLy_plan_new.md) (creates new instances)
  - [PLy_plan_dealloc](PLy_plan_dealloc.md) (deallocates instances)
  - [PLy_spi_prepare](PLy_spi_prepare.md) (prepares plans)
  - [PLy_spi_execute_plan](PLy_spi_execute_plan.md) (executes plans)
  - [PLy_cursor_plan](PLy_cursor_plan.md) (creates cursors from plans)

## Notes and Other Information
- This structure is the foundation of prepared statement functionality in PL/Python, enabling efficient repeated execution of SQL with different parameters
- Memory management is handled through the mcxt field, which ensures proper cleanup when the plan object is deallocated
- The structure supports the Python object protocol, allowing it to be used seamlessly within Python code as a PLyPlan object
- Type conversion between Python and PostgreSQL types is handled through the args array of PLyObToDatum converters
- Plans are created through PLy_spi_prepare() and can be executed via PLy_spi_execute_plan() or used to create cursors via PLy_cursor_plan()