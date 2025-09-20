# PLySubtransactionObject

## Location
[src/pl/plpython/plpy_subxactobject.h:16-21](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_subxactobject.h#L16-L21)

## Overview
PLySubtransactionObject is a Python object structure that represents a subtransaction in the PL/Python procedural language extension for PostgreSQL.

## Definition

```c
typedef struct PLySubtransactionObject
{
	PyObject_HEAD
	bool		started;
	bool		exited;
} PLySubtransactionObject;
```
## Detailed Description
PLySubtransactionObject is a Python extension type that provides a Python interface for PostgreSQL subtransactions within PL/Python functions. This structure inherits from Python's base object type (PyObject_HEAD) and maintains state information about the subtransaction lifecycle. It allows Python code to explicitly control subtransaction boundaries, enabling atomic execution of multiple SPI (Server Programming Interface) calls with controllable exception handling.

The object follows Python's context manager protocol, allowing it to be used with Python's 'with' statement for automatic resource management. When a subtransaction is started, it creates a new transaction context that can be independently committed or rolled back without affecting the parent transaction.

## Parameters / Member Variables
- `PyObject_HEAD`: Standard Python object header containing reference count and type information
- `started`: Boolean flag indicating whether the subtransaction has been initiated via enter() method
- `exited`: Boolean flag indicating whether the subtransaction has been terminated via exit() method

## Dependencies
- Functions called/Symbols referenced:
  - PyObject_HEAD (Python C API)
- Called from (representative examples):
  - [PLy_subtransaction_new](PLy_subtransaction_new.md)
  - [PLy_subtransaction_enter](PLy_subtransaction_enter.md)
  - [PLy_subtransaction_exit](PLy_subtransaction_exit.md)

## Notes and Other Information
- The object is created through PLy_subtransaction_new() function when plpy.subtransaction() is called in Python
- State transitions: created -> started -> exited (one-way progression)
- Both started and exited flags are used to prevent invalid state transitions (e.g., entering an already started subtransaction)
- The object supports Python's context manager protocol for use with 'with' statements
- Memory management follows Python's reference counting system
- Part of PostgreSQL's PL/Python extension located in src/pl/plpython/