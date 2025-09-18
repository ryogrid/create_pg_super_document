# AlterTSConfigType

## Location
src/include/nodes/parsenodes.h: 4112 - 4113

## Overview
AlterTSConfigType is an enumeration that specifies the type of alteration operation to perform on a text search configuration in PostgreSQL.

## Definition


## Detailed Description
This enumeration defines the different types of modifications that can be made to PostgreSQL text search configurations through the ALTER TEXT SEARCH CONFIGURATION statement. Text search configurations define how documents are processed for full-text search by specifying which dictionaries to use for different types of tokens. Each enum value represents a specific type of mapping manipulation within a text search configuration.

## Parameters / Member Variables
- : Add a new token type to dictionary mapping
- : Modify the dictionary mapping for a specific token type
- : Replace one dictionary with another in all mappings
- : Replace a dictionary for a specific token type
- : Remove a token type mapping from the configuration

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - AlterTSConfigurationStmt (as the 'kind' field)
  - Parser grammar rules in gram.y for ALTER TEXT SEARCH CONFIGURATION statements

## Notes and Other Information
- This enum is part of PostgreSQL's full-text search infrastructure
- Used specifically for ALTER TEXT SEARCH CONFIGURATION SQL statements
- Text search configurations are part of PostgreSQL's advanced text search capabilities
- The enum works in conjunction with AlterTSConfigurationStmt structure to represent parsed ALTER TEXT SEARCH CONFIGURATION commands
- Located in src/include/nodes/parsenodes.h as part of the SQL parsing framework