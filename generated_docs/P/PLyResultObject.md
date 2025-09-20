# PLyResultObject

## Location
[src/pl/plpython/plpy_resultobject.h:13-22](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_resultobject.h#L13-L22)

## Overview
PLyResultObject is a C structure that represents the result set of a PostgreSQL query execution in the PL/Python procedural language extension, serving as the bridge between PostgreSQL query results and Python objects.

## Definition

```c
typedef struct PLyResultObject
{
	PyObject_HEAD
	/* HeapTuple *tuples; */
	PyObject   *nrows;			/* number of rows returned by query */
	PyObject   *rows;			/* data rows, or empty list if no data
								 * returned */
	PyObject   *status;			/* query status, SPI_OK_*, or SPI_ERR_* */
	TupleDesc	tupdesc;
} PLyResultObject;
```
## Detailed Description
PLyResultObject is a Python C extension object that encapsulates the results of SQL query execution within PL/Python functions. It extends the standard Python object model (PyObject_HEAD) to provide a Python-accessible interface to PostgreSQL query results. This structure is used to return query results from plpy.execute() and related functions to Python code, allowing Python functions to access row data, metadata, and execution status information. The object supports both sequence and mapping protocols, enabling Python-style access to query results through indexing and iteration.

## Parameters / Member Variables
- : Standard Python object header containing reference count and type information
- : Python integer object representing the number of rows returned by the query (-1 for utility commands)
- : Python list object containing the actual row data as dictionaries, or empty list if no data returned
- : Python object representing the query execution status (SPI_OK_*, SPI_ERR_* constants)
- : PostgreSQL tuple descriptor containing metadata about the result columns (types, names, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - PyObject_HEAD (Python C API)
  - [TupleDesc](../T/TupleDesc.md) (PostgreSQL tuple descriptor)
  - PyObject (Python C API base object type)
  
- Called from (representative examples):
  - [PLy_result_new](PLy_result_new.md): Creates new PLyResultObject instances
  - [PLy_spi_execute_fetch_result](PLy_spi_execute_fetch_result.md): Populates result objects with query data
  - [PLy_cursor_fetch](PLy_cursor_fetch.md): Retrieves cursor results into PLyResultObject
  - [PLy_result_colnames](PLy_result_colnames.md): Accesses column name information
  - [PLy_result_nrows](PLy_result_nrows.md): Returns number of rows
  - [PLy_result_status](PLy_result_status.md): Returns execution status

## Notes and Other Information
- The structure includes a commented-out HeapTuple *tuples member, suggesting an earlier design that stored tuples directly
- Implements Python sequence and mapping protocols through PLy_result_as_sequence and PLy_result_as_mapping
- Supports Python methods: colnames(), coltypes(), coltypmods(), nrows(), status()
- Memory management follows Python reference counting conventions with Py_INCREF/Py_DECREF
- The tupdesc member is managed separately from Python's garbage collection and must be freed explicitly
- Used extensively in PL/Python's SPI (Server Programming Interface) integration
- Provides the primary mechanism for returning structured query results to Python code in PostgreSQL stored procedures