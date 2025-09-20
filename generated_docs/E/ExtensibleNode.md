# ExtensibleNode

## Location
[src/include/nodes/extensible.h:32-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/extensible.h#L32-L38)

## Overview
ExtensibleNode is a base structure that allows PostgreSQL extensions to define new types of nodes within the query tree while integrating seamlessly with PostgreSQL's node system.

## Definition

```c
typedef struct ExtensibleNode
{
	pg_node_attr(custom_copy_equal, custom_read_write)

	NodeTag		type;
	const char *extnodename;	/* identifier of ExtensibleNodeMethods */
} ExtensibleNode;
```
## Detailed Description
ExtensibleNode provides a framework for PostgreSQL extensions to create custom node types that can be integrated into the query planning and execution system. The structure always uses the T_ExtensibleNode NodeTag, while the extnodename field serves as a unique identifier that can be looked up to find the corresponding ExtensibleNodeMethods structure containing the callback functions for handling this specific node type.

The pg_node_attr annotation indicates that this node type uses custom implementations for copy, equality comparison, and read/write operations rather than the standard generated ones.

## Parameters / Member Variables
- `type`: Always set to T_ExtensibleNode to identify this as an extensible node
- `*extnodename`: A string identifier that uniquely identifies the specific type of extensible node and is used to look up the corresponding ExtensibleNodeMethods
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (type system)
  - pg_node_attr (attribute system)
- Called from (representative examples):
  - [_copyExtensibleNode](../c/_copyExtensibleNode.md) (copy functions)
  - [_equalExtensibleNode](../e/_equalExtensibleNode.md) (equality functions) 
  - [_outExtensibleNode](../o/_outExtensibleNode.md) (output functions)
  - [_readExtensibleNode](../r/_readExtensibleNode.md) (read functions)

## Notes and Other Information
- This is the base structure for all extensible nodes - extensions should embed this as the first member of their custom node structures
- The extnodename field must correspond to a registered ExtensibleNodeMethods structure
- All extensible nodes share the same NodeTag (T_ExtensibleNode) but are differentiated by their extnodename
- The custom_copy_equal and custom_read_write attributes ensure that the node system will use the callbacks defined in ExtensibleNodeMethods rather than auto-generated functions
- This design allows extensions to extend PostgreSQL's node system without modifying core code