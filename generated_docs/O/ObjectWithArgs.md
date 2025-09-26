# ObjectWithArgs

## Location
src/include/nodes/parsenodes.h: 2524 - 2531

## Overview
ObjectWithArgs represents a function, procedure, or operator name combined with parameter identification, providing a complete specification for objects that can be overloaded based on their argument signatures.

## Definition

```c
typedef struct ObjectWithArgs
{
	NodeTag		type;
	List	   *objname;		/* qualified name of function/operator */
	List	   *objargs;		/* list of Typename nodes (input args only) */
	List	   *objfuncargs;	/* list of FunctionParameter nodes */
	bool		args_unspecified;	/* argument list was omitted? */
} ObjectWithArgs;
```
## Detailed Description
ObjectWithArgs is a fundamental parse tree node structure in PostgreSQL that addresses the complexity of identifying overloaded database objects like functions, procedures, and operators. In PostgreSQL, these objects can have multiple definitions with the same name but different parameter signatures, making parameter information essential for unambiguous identification.

The structure provides two levels of parameter specification: objargs contains only input parameter types (following traditional PostgreSQL lookup rules), while objfuncargs provides the complete parameter specification including parameter modes, names, and default values. This dual approach supports both legacy compatibility and modern extended functionality.

The args_unspecified flag handles cases where SQL syntax allows parameter lists to be omitted entirely, enabling lookup by name alone when the object name is unique in its namespace. This flexibility accommodates different SQL language constructs while maintaining precise object identification when needed.

## Parameters / Member Variables
- : NodeTag for node type identification in PostgreSQL's node system
- : List of strings representing the qualified name of the function or operator (e.g., schema.function_name)
- : List of Typename nodes specifying only the input parameter types, used for traditional PostgreSQL object lookup rules
- : List of FunctionParameter nodes providing complete parameter specifications including modes, names, and defaults; NIL if not needed
- : Boolean flag indicating whether the argument list was entirely omitted in the original SQL, enabling name-only lookup for unique objects

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this symbol)
- Called from (representative examples):
  - objectNamesToOids
  - get_object_address  
  - pg_get_object_address
  - RemoveObjects
  - LookupFuncWithArgs
  - LookupOperWithArgs

## Notes and Other Information
- Part of PostgreSQL's parse tree node system, inheriting from the standard Node structure
- Critical for PostgreSQL's function and operator overloading system, which allows multiple objects with the same name but different signatures
- Used extensively in DDL operations like DROP FUNCTION, ALTER FUNCTION, and GRANT statements
- The dual parameter representation (objargs vs objfuncargs) supports both simple and complex parameter specifications
- Essential for the object addressing system that provides uniform identification of database objects
- Supports PostgreSQL's sophisticated namespace and overloading rules for callable objects
- When args_unspecified is true, the lookup mechanism will only succeed if the object name is unique within its namespace
- Used in conjunction with PostgreSQL's type system to resolve parameter type specifications