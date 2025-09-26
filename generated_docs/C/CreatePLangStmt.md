# CreatePLangStmt

## Location
[src/include/nodes/parsenodes.h:3054-3063](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3054-L3063)

## Overview
CreatePLangStmt is a parse tree node structure that represents the CREATE LANGUAGE SQL statement, used to define procedural languages within PostgreSQL.

## Definition

```c
typedef struct CreatePLangStmt
{
	NodeTag		type;
	bool		replace;		/* T => replace if already exists */
	char	   *plname;			/* PL name */
	List	   *plhandler;		/* PL call handler function (qual. name) */
	List	   *plinline;		/* optional inline function (qual. name) */
	List	   *plvalidator;	/* optional validator function (qual. name) */
	bool		pltrusted;		/* PL is trusted */
} CreatePLangStmt;
```
## Detailed Description
CreatePLangStmt is a parser node structure that encapsulates all the information needed to create a procedural language in PostgreSQL. This structure is created during SQL parsing when a CREATE LANGUAGE statement is encountered. It holds all the parameters and options that define how a procedural language should be installed and configured in the database system.

The structure supports both creating new languages and replacing existing ones through the 'replace' flag. It specifies the essential components of a procedural language: the handler function (required), optional inline and validator functions, and whether the language should be trusted or untrusted.

## Parameters / Member Variables
- : Standard NodeTag identifying this as a CreatePLangStmt node
- : Boolean flag indicating whether to replace an existing language with the same name (CREATE OR REPLACE LANGUAGE)
- : String containing the name of the procedural language to be created
- : List containing the qualified name of the language's call handler function
- : List containing the qualified name of the optional inline function for the language
- : List containing the qualified name of the optional validator function for the language
- : Boolean flag indicating whether the language is trusted (can be used by non-superusers)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - List (for storing qualified function names)
- Called from (representative examples):
  - CreateProceduralLanguage (command execution)
  - ProcessUtilitySlow (utility command processing)

## Notes and Other Information
- This structure is part of the parse tree node hierarchy and follows PostgreSQL's standard node conventions
- The plhandler, plinline, and plvalidator fields use List structures to store qualified names, allowing for schema-qualified function references
- Trusted languages can be used by regular users, while untrusted languages require superuser privileges
- The structure is defined in src/include/nodes/parsenodes.h alongside other DDL statement nodes
- Location: src/include/nodes/parsenodes.h:3054-3063