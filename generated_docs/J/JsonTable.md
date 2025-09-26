# JsonTable

## Location
[src/include/nodes/parsenodes.h:1821-1832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1821-L1832)

## Overview
JsonTable represents the untransformed parse tree representation of a JSON_TABLE expression, which allows querying JSON data as relational table rows and columns in PostgreSQL.

## Definition
```c
typedef struct JsonTable
{
    NodeTag             type;
    JsonValueExpr      *context_item;    /* context item expression */
    JsonTablePathSpec  *pathspec;       /* JSON path specification */
    List               *passing;        /* list of PASSING clause arguments, if any */
    List               *columns;        /* list of JsonTableColumn */
    JsonBehavior       *on_error;      /* ON ERROR behavior */
    Alias              *alias;         /* table alias in FROM clause */
    bool                lateral;        /* does it have LATERAL prefix? */
    ParseLoc            location;       /* token location, or -1 if unknown */
} JsonTable;
```

## Detailed Description
JsonTable is a parse node that represents the SQL/JSON JSON_TABLE function, which provides a way to extract data from JSON documents and present it as a relational table. This structure contains all the components needed to define how JSON data should be transformed into tabular form, including the context expression, path specification, column definitions, and error handling behavior. It serves as the untransformed representation before being converted into execution plan nodes.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a JsonTable node
- `context_item`: JsonValueExpr representing the JSON data source to be queried
- `pathspec`: JsonTablePathSpec defining the JSON path for row generation
- `passing`: List of expressions passed to the JSON path evaluation context
- `columns`: List of JsonTableColumn structures defining the output columns
- `on_error`: JsonBehavior specifying how to handle errors during processing
- `alias`: Table alias used in FROM clause for referencing the result
- `lateral`: Boolean indicating whether LATERAL keyword was specified
- `location`: ParseLoc for tracking the position in the source query

## Dependencies
- Functions called/Symbols referenced:
  - JsonValueExpr (context item expression)
  - JsonTablePathSpec (path specification)
  - JsonBehavior (error handling behavior)
  - Alias (table aliasing)
  - ParseLoc (location tracking)
- Called from (representative examples):
  - transformFromClauseItem (FROM clause processing)
  - transformJsonTable (JSON table transformation)
  - transformJsonTableColumns (column processing)

## Notes and Other Information
- Central structure for SQL/JSON JSON_TABLE functionality in PostgreSQL
- The LATERAL keyword allows the JSON_TABLE to reference columns from preceding FROM clause items  
- Error handling behavior can be customized through the on_error field
- Supports complex JSON path expressions for flexible data extraction
- Part of the SQL/JSON standard implementation in PostgreSQL