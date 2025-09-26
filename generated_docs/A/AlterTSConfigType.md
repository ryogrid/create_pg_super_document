# AlterTSConfigType

## Location
[src/include/nodes/parsenodes.h:4112-4113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4112-L4113)

## Overview
AlterTSConfigType is an enumeration that specifies the type of alteration operation to perform on a text search configuration in PostgreSQL.

## Definition

```c
typedef struct AlterTSConfigurationStmt
{
	NodeTag		type;
	AlterTSConfigType kind;		/* ALTER_TSCONFIG_ADD_MAPPING, etc */
	List	   *cfgname;		/* qualified name (list of String) */

	/*
	 * dicts will be non-NIL if ADD/ALTER MAPPING was specified. If dicts is
	 * NIL, but tokentype isn't, DROP MAPPING was specified.
	 */
	List	   *tokentype;		/* list of String */
	List	   *dicts;			/* list of list of String */
	bool		override;		/* if true - remove old variant */
	bool		replace;		/* if true - replace dictionary by another */
	bool		missing_ok;		/* for DROP - skip error if missing? */
} AlterTSConfigurationStmt;
```
## Detailed Description
This enumeration defines the different types of modifications that can be made to PostgreSQL text search configurations through the ALTER TEXT SEARCH CONFIGURATION statement. Text search configurations define how documents are processed for full-text search by specifying which dictionaries to use for different types of tokens. Each enum value represents a specific type of mapping manipulation within a text search configuration.

## Parameters / Member Variables
- `ALTER_TSCONFIG_ADD_MAPPING`: Add a new token type to dictionary mapping
- `ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN`: Modify the dictionary mapping for a specific token type
- `ALTER_TSCONFIG_REPLACE_DICT`: Replace one dictionary with another in all mappings
- `ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN`: Replace a dictionary for a specific token type
- `ALTER_TSCONFIG_DROP_MAPPING`: Remove a token type mapping from the configuration

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - [AlterTSConfigurationStmt](AlterTSConfigurationStmt.md) (as the 'kind' field)
  - Parser grammar rules in gram.y for ALTER TEXT SEARCH CONFIGURATION statements

## Notes and Other Information
- This enum is part of PostgreSQL's full-text search infrastructure
- Used specifically for ALTER TEXT SEARCH CONFIGURATION SQL statements
- Text search configurations are part of PostgreSQL's advanced text search capabilities
- The enum works in conjunction with AlterTSConfigurationStmt structure to represent parsed ALTER TEXT SEARCH CONFIGURATION commands
- Located in src/include/nodes/parsenodes.h as part of the SQL parsing framework