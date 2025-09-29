# _outList

## Location
[src/backend/nodes/outfuncs.c:275-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L275-L324)

## Overview
Serializes PostgreSQL List structures into a parenthesized format, handling different list types (generic node lists, integer lists, OID lists, and XID lists) with appropriate type indicators and formatting.

## Definition
static void _outList(StringInfo str, const List *node)

## Detailed Description
The _outList function is a critical component of PostgreSQL's node serialization system that handles the output of various list types. PostgreSQL uses different specialized list types for different data types to optimize memory usage and access patterns.

The function begins by outputting an opening parenthesis, then determines the type of list and adds appropriate type indicators:
- Generic node lists (List): no special indicator
- Integer lists (IntList): 'i' indicator  
- OID lists (OidList): 'o' indicator
- XID lists (XidList): 'x' indicator

For each element in the list, the function uses type-specific formatting:
- Generic node lists: Calls outNode() recursively for each node, with spaces between elements for backward compatibility
- Integer lists: Outputs integers with "%d" format preceded by a space
- OID lists: Outputs OIDs with "%u" format preceded by a space  
- XID lists: Outputs XIDs with "%u" format preceded by a space

The function maintains backward compatibility by using slightly different whitespace formatting for generic node lists versus other list types. Finally, it closes the serialized list with a closing parenthesis.

## Parameters / Member Variables
- `str`: StringInfo buffer where the serialized list will be appended
- `node`: Const pointer to the List structure to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfoChar](../a/appendStringInfoChar.md) (for parentheses and type indicators)
  - IsA (macro for type checking)
  - [outNode](outNode.md) (recursive function for serializing node elements)
  - [lnext](../l/lnext.md) (macro for list traversal)
  - lfirst_int (macro for extracting integer values)
  - lfirst_oid (macro for extracting OID values)  
  - lfirst_xid (macro for extracting XID values)
  - [appendStringInfo](../a/appendStringInfo.md) (for formatted integer/OID/XID output)
  - elog (for error reporting)

- Called from (representative examples):
  - [outNode](outNode.md) (main node output dispatcher in outfuncs.c:725)

## Notes and Other Information
- This function is declared static, limiting its scope to the outfuncs.c file
- Handles four distinct list types with different serialization strategies for performance optimization
- Maintains backward compatibility with different whitespace formatting for different list types
- Uses type indicators to allow the parser to correctly reconstruct the appropriate list type
- The foreach macro provides safe iteration over list cells
- Error handling ensures that unrecognized list types are caught at runtime
- Part of the broader node serialization infrastructure that enables query plan storage, debugging, and transmission

## Simplified Source

```c
static void _outList(StringInfo str, const List *node)
{
    const ListCell *lc;

    // Start with opening parenthesis
    appendStringInfoChar(str, '(');

    // Add type indicator for specialized lists
    if (IsA(node, IntList))
        appendStringInfoChar(str, 'i');
    else if (IsA(node, OidList))
        appendStringInfoChar(str, 'o');
    else if (IsA(node, XidList))
        appendStringInfoChar(str, 'x');

    // Process each list element
    foreach(lc, node)
    {
        if (IsA(node, List))
        {
            // Generic node list: serialize node + space
            outNode(str, lfirst(lc));
            if (lnext(node, lc))
                appendStringInfoChar(str, ' ');
        }
        else if (IsA(node, IntList))
            appendStringInfo(str, " %d", lfirst_int(lc));
        else if (IsA(node, OidList))
            appendStringInfo(str, " %u", lfirst_oid(lc));
        else if (IsA(node, XidList))
            appendStringInfo(str, " %u", lfirst_xid(lc));
        else
            elog(ERROR, "unrecognized list node type: %d", (int) node->type);
    }

    // End with closing parenthesis
    appendStringInfoChar(str, ')');
}
```