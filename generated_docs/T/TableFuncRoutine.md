# TableFuncRoutine

## Location
src/include/executor/tablefunc.h: 52 - 65

## Overview
TableFuncRoutine is a structure containing function pointers that define the interface for generating content of table-producer functions such as XMLTABLE, providing a standardized way to build and extract rows from structured data sources.

## Definition

```c
typedef struct TableFuncRoutine
{
	void		(*InitOpaque) (struct TableFuncScanState *state, int natts);
	void		(*SetDocument) (struct TableFuncScanState *state, Datum value);
	void		(*SetNamespace) (struct TableFuncScanState *state, const char *name,
								 const char *uri);
	void		(*SetRowFilter) (struct TableFuncScanState *state, const char *path);
	void		(*SetColumnFilter) (struct TableFuncScanState *state,
									const char *path, int colnum);
	bool		(*FetchRow) (struct TableFuncScanState *state);
	Datum		(*GetValue) (struct TableFuncScanState *state, int colnum,
							 Oid typid, int32 typmod, bool *isnull);
	void		(*DestroyOpaque) (struct TableFuncScanState *state);
} TableFuncRoutine;
```
## Detailed Description
TableFuncRoutine serves as a plugin-style interface for implementing table-producing functions in PostgreSQL. It abstracts the common operations needed to process structured documents (like XML or JSON) and extract tabular data from them. The structure provides a complete lifecycle management system for table builders, from initialization through data extraction to cleanup.

The routine operates in phases: first initializing private state, then configuring the document source and extraction filters, followed by iterative row fetching and value extraction, and finally cleanup. This design allows different table function implementations (like XMLTABLE, JSON_TABLE) to plug into the same executor infrastructure while providing their own specific parsing and extraction logic.

Each function pointer represents a specific phase or operation in the table generation process, with the TableFuncScanState serving as the shared context that maintains state across all operations.

## Parameters / Member Variables
- : Initializes table builder private objects with tuple descriptor, input functions, and type parameters from executor state
- : Defines the input document for processing, allowing additional transformations within the table builder context
- : Passes namespace declarations from table expressions (may be NULL if namespaces not supported); must be called before setting row/column filters
- : Defines the row-generating filter used to extract each row from the input document
- : Called once per column to define the column-generating filter for the specified column
- : Called repeatedly until no more rows found; sets up state for subsequent GetValue calls to return column values for current row
- : Returns the value for the specified column of the current row, with proper type conversion and null handling
- : Releases all resources associated with table builder context (called on completion or error)

## Dependencies
- Functions called/Symbols referenced:
  - TableFuncScanState (primary state structure)
  - Datum (PostgreSQL data type)
  - Oid (object identifier type)
  - int32 (type modifier)
  
- Called from (representative examples):
  - tfuncFetchRows (nodeTableFuncscan.c:270)
  - tfuncInitialize (nodeTableFuncscan.c:342)  
  - tfuncLoadRows (nodeTableFuncscan.c:437)

## Notes and Other Information
- The routine provides a clean separation between the executor logic and specific document format handling
- Namespace handling is optional - SetNamespace may be NULL if the implementation doesn't support namespaces
- The default namespace can be specified by passing NULL as the name parameter to SetNamespace
- Error handling during any phase should result in DestroyOpaque being called to prevent resource leaks
- The design supports streaming processing where rows are fetched on-demand rather than materializing the entire result set
- This interface is used by table functions like XMLTABLE to integrate with PostgreSQL's execution engine while maintaining format-specific parsing logic