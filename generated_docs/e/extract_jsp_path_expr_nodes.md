# extract_jsp_path_expr_nodes

## Location
[src/backend/utils/adt/jsonb_gin.c:504-563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L504-L563)

## Overview
Extracts a list of expression nodes from a JSON path expression that need to be AND-ed together, processing path items and filter expressions for GIN index queries.

## Definition
```c
static List *
extract_jsp_path_expr_nodes(JsonPathGinContext *cxt, JsonPathGinPath path,
                            JsonPathItem *jsp, JsonbValue *scalar)
```

## Detailed Description
This function traverses a JSON path expression and extracts indexable nodes for GIN operations. It processes each path item in sequence, handling current position operators, filter expressions, and other path elements. For filter expressions (jpiFilter), it extracts boolean expressions that can be used for index scanning. The function delegates path item processing to the context's add_path_item function and stops processing if an unsupported path item is encountered. Finally, it calls the context's extract_nodes function to append nodes from the path expression itself to any filter nodes that were extracted.

## Parameters / Member Variables
- `cxt`: JsonPathGinContext containing the extraction context and operator class-specific functions
- `path`: JsonPathGinPath representing the current path being processed
- `jsp`: JsonPathItem pointer to the JSON path item to process
- `scalar`: JsonbValue pointer to the scalar value for equality queries (can be NULL for EXISTS queries)

## Dependencies
- Functions called/Symbols referenced:
  - [jspGetArg](../j/jspGetArg.md)
  - [extract_jsp_bool_expr](extract_jsp_bool_expr.md)
  - [jspGetNext](../j/jspGetNext.md)
  - [lappend](../l/lappend.md)
  - JsonPathItem
  - [JsonPathGinNode](../J/JsonPathGinNode.md)
  - [JsonPathGinContext](../J/JsonPathGinContext.md)
  - JsonPathGinPath
  - Various enum values (jpiCurrent, jpiFilter)
- Called from (representative examples):
  - [extract_jsp_path_expr](extract_jsp_path_expr.md)

## Notes and Other Information
- This is a static function within the JSONB GIN indexing module
- Handles the traversal and processing of JSON path expressions for index extraction
- Processes filter expressions by extracting their boolean conditions
- Stops processing when encountering unsupported path items
- The function can extract both equality conditions (when scalar is provided) and existence conditions (when scalar is NULL)
- Part of PostgreSQL's GIN indexing infrastructure for efficient JSONB path queries
- Located in src/backend/utils/adt/jsonb_gin.c:504-563

## Simplified Source

```c
static List *
extract_jsp_path_expr_nodes(JsonPathGinContext *cxt, JsonPathGinPath path,
                            JsonPathItem *jsp, JsonbValue *scalar)
{
    JsonPathItem next;
    List *nodes = NIL;

    // Process each path item in sequence
    for (;;)
    {
        switch (jsp->type)
        {
            case jpiCurrent:
                // Current position operator - continue processing
                break;

            case jpiFilter:
                {
                    // Extract filter expression as boolean node
                    JsonPathItem arg;
                    JsonPathGinNode *filter;

                    jspGetArg(jsp, &arg);
                    filter = extract_jsp_bool_expr(cxt, path, &arg, false);

                    if (filter)
                        nodes = lappend(nodes, filter);
                    break;
                }

            default:
                // Process path item via context-specific handler
                if (!cxt->add_path_item(&path, jsp))
                    // Unsupported path - return filter nodes only
                    return nodes;
                break;
        }

        // Move to next path item
        if (!jspGetNext(jsp, &next))
            break;
        jsp = &next;
    }

    // Add nodes from path expression to filter nodes
    return cxt->extract_nodes(cxt, path, scalar, nodes);
}
```