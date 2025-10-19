# dump_node

## Location
[src/backend/utils/adt/formatting.c:1480-1515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1480-L1515)

## Overview
A debugging utility function that dumps the structure and contents of a FormatNode array to the debug log for diagnostic purposes in PostgreSQL's date/time formatting system.

## Definition

```c
static void
dump_node(FormatNode *node, int max)
```
## Detailed Description
The  function is a static debugging helper used within PostgreSQL's formatting system (src/backend/utils/adt/formatting.c). It iterates through an array of FormatNode structures and outputs detailed information about each node to the debug log using . This function is primarily used for troubleshooting and understanding the internal structure of format parsing trees during development and debugging of the to_char/to_date functions.

The function traverses the FormatNode array from the starting node up to the specified maximum index, printing different information based on the node type:
- For ACTION nodes: displays the node's key name and suffix information
- For CHAR nodes: shows the character content
- For END nodes: indicates the end of the format structure and terminates iteration
- For unknown node types: reports an error

## Parameters / Member Variables
- `*node`: Pointer to the first FormatNode in the array to be dumped
- `max`: Maximum index to traverse in the FormatNode array (0-based indexing)
## Dependencies
- Functions called/Symbols referenced:
  - [FormatNode](../F/FormatNode.md) (struct type)
  - elog
  - DEBUG_elog_output
  - NODE_TYPE_ACTION
  - NODE_TYPE_CHAR  
  - NODE_TYPE_END
  - DUMP_THth
  - DUMP_FM
- Called from (representative examples):
  - DCH_ZONED (indirectly through debugging macros)

## Notes and Other Information
- This is a static function only available within the formatting.c compilation unit
- The function is primarily used for debugging and is conditionally compiled based on debug settings
- The DUMP_THth and DUMP_FM macros are used to display suffix information in a readable format
- The function will terminate early when it encounters a NODE_TYPE_END node
- Output is sent to the PostgreSQL debug log system via elog() with DEBUG_elog_output level

## Simplified Source

```c
static void dump_node(FormatNode *node, int max) {
    // Debug function to dump FormatNode array structure
    elog(DEBUG_elog_output, "to_from-char(): DUMP FORMAT");

    // Iterate through nodes up to max index
    for (int a = 0; a <= max; a++, node++) {
        // Handle different node types
        if (node->type == NODE_TYPE_ACTION) {
            // Action node: show key name and suffix info
            elog(DEBUG_elog_output, "%d:\t NODE_TYPE_ACTION '%s'\t(%s,%s)",
                 a, node->key->name, DUMP_THth(node->suffix), DUMP_FM(node->suffix));
        }
        else if (node->type == NODE_TYPE_CHAR) {
            // Character node: show character content
            elog(DEBUG_elog_output, "%d:\t NODE_TYPE_CHAR '%s'", a, node->character);
        }
        else if (node->type == NODE_TYPE_END) {
            // End node: terminate iteration
            elog(DEBUG_elog_output, "%d:\t NODE_TYPE_END", a);
            return;
        }
        else {
            // Unknown node type
            elog(DEBUG_elog_output, "%d:\t unknown NODE!", a);
        }
    }
}
```