SystemAttributeByName

## Overview
SystemAttributeByName searches for a system attribute definition by name and returns the corresponding Form_pg_attribute pointer, or NULL if not found.

## Definition
const FormData_pg_attribute * SystemAttributeByName(const char *attname)

## Detailed Description
SystemAttributeByName performs a linear search through the SysAtt array to find a system attribute that matches the provided attribute name. This function complements SystemAttributeDefinition by allowing lookup by name rather than by attribute number. It iterates through all predefined system attributes (ctid, xmin, cmin, xmax, cmax, tableoid) and performs string comparison to find a match.

The function uses strcmp to perform case-sensitive name comparison and returns the first matching system attribute definition, or NULL if no match is found.

## Parameters / Member Variables
- attname: The name of the system attribute to search for (case-sensitive). Valid names include "ctid", "xmin", "cmin", "xmax", "cmax", and "tableoid".

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro to get array length)
  - strcmp (string comparison function)
  - NameStr (macro to extract string from Name type)
  - SysAtt (static array of system attribute definitions)
- Called from (representative examples):
  - [CheckAttributeNamesTypes](../C/CheckAttributeNamesTypes.md) (in src/backend/catalog/heap.c:483)
  - [SPI_fnumber](SPI_fnumber.md) (in src/backend/executor/spi.c:1189)
  - [specialAttNum](../s/specialAttNum.md) (in src/backend/parser/parse_relation.c:3518)
  - [transformIndexConstraint](../t/transformIndexConstraint.md) (in src/backend/parser/parse_utilcmd.c:2454, 2597)

## Notes and Other Information
- Performs linear search through the 6 predefined system attributes
- Returns NULL for non-system attribute names, allowing callers to distinguish between system and user-defined attributes
- Case-sensitive string matching using strcmp
- Commonly used in SQL parsing and attribute resolution contexts
- Complements SystemAttributeDefinition which works with attribute numbers instead of names
- Located in src/backend/catalog/heap.c:253-266