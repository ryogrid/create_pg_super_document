# AlterTSDictionaryStmt

## Location
src/include/nodes/parsenodes.h: 4095 - 4100

## Overview
AlterTSDictionaryStmt represents the parsed representation of an ALTER TEXT SEARCH DICTIONARY statement, which modifies the configuration options of an existing text search dictionary.

## Definition
```c
typedef struct AlterTSDictionaryStmt
{
    NodeTag     type;
    List       *dictname;      /* qualified name (list of String) */
    List       *options;       /* List of DefElem nodes */
} AlterTSDictionaryStmt;
```

## Detailed Description
The AlterTSDictionaryStmt structure is a parse node that encapsulates the information needed to execute an ALTER TEXT SEARCH DICTIONARY statement in PostgreSQL. Text search dictionaries are components of PostgreSQL's full-text search system that process tokens produced by parsers, typically to normalize words, remove stop words, or apply stemming. This statement allows modification of dictionary-specific configuration options after the dictionary has been created.

The structure follows PostgreSQL's standard parse node pattern and contains the dictionary name (which may be schema-qualified) and a list of configuration options to be altered. These options are dictionary-type specific and control how the dictionary processes text tokens.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an AlterTSDictionaryStmt parse node
- `dictname`: List of String nodes representing the qualified name of the text search dictionary to alter (e.g., ['myschema', 'mydict'] for myschema.mydict)
- `options`: List of DefElem nodes containing the configuration options to set or modify for the dictionary

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for parse node identification)
  - List (PostgreSQL's generic list structure)
  - DefElem (structure for definition elements containing option names and values)

- Called from (representative examples):
  - AlterTSDictionary (executes the ALTER TEXT SEARCH DICTIONARY command)
  - ProcessUtilitySlow (utility command processing dispatcher)

## Notes and Other Information
- Text search dictionaries are part of PostgreSQL's full-text search framework, working in conjunction with parsers and configurations
- The available options depend on the specific dictionary template used when the dictionary was created (e.g., snowball, simple, synonym, thesaurus)
- Dictionary names can be schema-qualified, and if no schema is specified, the dictionary is looked up in the current search path
- This statement is used to modify existing dictionaries; for creating new dictionaries, CREATE TEXT SEARCH DICTIONARY is used
- The DefElem nodes in the options list contain key-value pairs representing the configuration parameters specific to the dictionary type
- Part of PostgreSQL's text search system, defined in the parsenodes.h header file