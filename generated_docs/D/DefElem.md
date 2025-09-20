# DefElem

## Location
[src/include/nodes/parsenodes.h:811-820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L811-L820)

## Overview
DefElem represents a generic definition element used throughout PostgreSQL's DDL commands to specify named options, parameters, and attributes with associated values and actions.

## Definition

```c
typedef struct DefElem
{
	NodeTag		type;
	char	   *defnamespace;	/* NULL if unqualified name */
	char	   *defname;
	Node	   *arg;			/* typically Integer, Float, String, or
								 * TypeName */
	DefElemAction defaction;	/* unspecified action, or SET/ADD/DROP */
	ParseLoc	location;		/* token location, or -1 if unknown */
} DefElem;
```
## Detailed Description
DefElem is one of the most widely used structures in PostgreSQL's parser, serving as a generic container for option-value pairs in DDL statements. It represents named parameters, configuration options, and attributes that can be specified in various SQL commands like CREATE, ALTER, and other data definition statements. The structure supports namespace qualification, various data types for values, and different actions (SET/ADD/DROP) making it extremely versatile for representing database object options and configurations.

## Parameters / Member Variables
- `type`: NodeTag identifier for this node type
- `*defnamespace`: Namespace qualifier for the option name (NULL for unqualified names)
- `*defname`: The name of the option or parameter being defined
- `*arg`: The value associated with the option (typically Integer, Float, String, or TypeName nodes)
- `defaction`: Specifies the action to perform (unspecified, SET, ADD, or DROP)
- `location`: Source location in the original SQL text (-1 if unknown)
## Dependencies
- Functions called/Symbols referenced:
  - [DefElemAction](DefElemAction.md) (enum for specifying the action type)
  - ParseLoc (type for tracking source location)
- Called from (representative examples):
  - [transformRelOptions](../t/transformRelOptions.md) (processes table/index options)
  - [DefineAggregate](DefineAggregate.md) (handles aggregate function definitions)
  - [DefineCollation](DefineCollation.md) (processes collation definitions)
  - [CreateExtension](../C/CreateExtension.md) (handles extension options)
  - [defGetString](../d/defGetString.md)/defGetNumeric/defGetBoolean (utility functions for extracting values)

## Notes and Other Information
DefElem is fundamental to PostgreSQL's extensible option system, allowing various database objects to accept custom parameters and configurations. The structure's flexibility enables consistent handling of options across different DDL commands while maintaining type safety through the arg Node pointer. The defaction field supports incremental modifications to option lists, particularly useful in ALTER statements where options can be added, modified, or removed independently.