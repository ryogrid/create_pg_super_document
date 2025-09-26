# AlterTSConfigurationStmt

## Location
[src/include/nodes/parsenodes.h:4114-4129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4114-L4129)

## Overview
AlterTSConfigurationStmt represents the parsed representation of an ALTER TEXT SEARCH CONFIGURATION statement, which modifies token-to-dictionary mappings in an existing text search configuration.

## Definition
```c
typedef struct AlterTSConfigurationStmt
{
    NodeTag         type;
    AlterTSConfigType kind;        /* ALTER_TSCONFIG_ADD_MAPPING, etc */
    List           *cfgname;       /* qualified name (list of String) */
    List           *tokentype;     /* list of String */
    List           *dicts;         /* list of list of String */
    bool            override;      /* if true - remove old variant */
    bool            replace;       /* if true - replace dictionary by another */
    bool            missing_ok;    /* for DROP - skip error if missing? */
} AlterTSConfigurationStmt;
```

## Detailed Description
The AlterTSConfigurationStmt structure is a parse node that encapsulates the information needed to execute an ALTER TEXT SEARCH CONFIGURATION statement in PostgreSQL. Text search configurations define how different token types (words, numbers, emails, etc.) produced by text search parsers are processed by mapping them to appropriate text search dictionaries. This statement allows modification of these token-to-dictionary mappings.

The structure supports several types of alterations: adding new token mappings (ADD MAPPING), modifying existing mappings (ALTER MAPPING), and removing mappings (DROP MAPPING). The kind field specifies which operation to perform, while the other fields provide the necessary data for that operation. The structure handles complex scenarios like replacing one dictionary with another or overriding existing mappings.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an AlterTSConfigurationStmt parse node
- `kind`: AlterTSConfigType enum value specifying the type of alteration (ADD_MAPPING, ALTER_MAPPING, or DROP_MAPPING)
- `cfgname`: List of String nodes representing the qualified name of the text search configuration to alter
- `tokentype`: List of String nodes specifying the token types to be affected by the alteration
- `dicts`: List of lists of String nodes representing the dictionaries to map to the token types (NULL for DROP MAPPING operations)
- `override`: Boolean flag indicating whether to remove existing mappings before adding new ones
- `replace`: Boolean flag indicating whether to replace an existing dictionary with another in the mapping
- `missing_ok`: Boolean flag for DROP operations - if true, don't raise an error if the mapping doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - [AlterTSConfigType](AlterTSConfigType.md) (enum defining the types of configuration alterations)
  - NodeTag (for parse node identification)
  - [List](../L/List.md) (PostgreSQL's generic list structure)

- Called from (representative examples):
  - [AlterTSConfiguration](AlterTSConfiguration.md) (executes the ALTER TEXT SEARCH CONFIGURATION command)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing dispatcher)  
  - [EventTriggerCollectAlterTSConfig](../E/EventTriggerCollectAlterTSConfig.md) (event trigger support for configuration changes)

## Notes and Other Information
- Text search configurations are central to PostgreSQL's full-text search system, determining how parsed tokens are processed by dictionaries
- The dicts field will be non-NULL for ADD/ALTER MAPPING operations but NULL for DROP MAPPING operations
- Token types correspond to those recognized by the text search parser associated with the configuration
- Multiple dictionaries can be mapped to a single token type, and they are consulted in order until one recognizes the token
- The override flag allows complete replacement of existing mappings rather than addition to them
- Configuration names can be schema-qualified, following standard PostgreSQL naming conventions
- Part of PostgreSQL's full-text search framework, defined in the parsenodes.h header file