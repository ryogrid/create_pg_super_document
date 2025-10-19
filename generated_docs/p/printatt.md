# printatt

## Location
[src/backend/access/common/printtup.c:423-443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/printtup.c#L423-L443)

## Overview
The printatt function outputs debug information about a PostgreSQL attribute to standard output, displaying its properties and value.

## Definition

```c
static void
printatt(unsigned attributeId,
		 Form_pg_attribute attributeP,
		 char *value)
```
## Detailed Description
The printatt function is a debugging utility that prints detailed information about a PostgreSQL table attribute. It formats and displays the attribute's metadata including its ID, name, value, type information, length, type modifier, and whether it's passed by value.

The function outputs a formatted line containing:
- The attribute ID (2-digit format)
- The attribute name
- The attribute value (if not NULL, enclosed in quotes)
- Type ID of the attribute
- Length of the attribute (-1 for variable length)
- Type modifier
- Whether the attribute is passed by value ('t' for true, 'f' for false)

This function is primarily used for debugging purposes to inspect the structure and content of tuples during development and troubleshooting.

## Parameters / Member Variables
- `attributeId`: Unsigned integer representing the position/ID of the attribute within the tuple
- `attributeP`: Pointer to Form_pg_attribute structure containing the attribute's metadata from the system catalog
- `*value`: Character pointer to the string representation of the attribute's value (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - printf: Standard C library function for formatted output
  - NameStr: PostgreSQL macro to extract name from a Name structure
- Called from (representative examples):
  - [debugStartup](../d/debugStartup.md): Uses printatt to display attribute information during startup debugging
  - [debugtup](../d/debugtup.md): Uses printatt to display tuple attribute information for debugging

## Notes and Other Information
- The function is marked as static, indicating it's only used within the printtup.c file
- This is a debugging function that outputs to stdout, not intended for production query result formatting
- The function safely handles NULL values by checking before attempting to print the value
- The output format is human-readable and includes technical details about PostgreSQL's internal attribute representation
- Type modifier (typmod) provides additional type-specific information (e.g., precision for numeric types)

## Simplified Source

```c
static void printatt(unsigned attributeId,
                    Form_pg_attribute attributeP,
                    char *value) {
    // Print formatted attribute information for debugging
    printf("\t%2d: %s%s%s%s\t(typeid = %u, len = %d, typmod = %d, byval = %c)\n",
           attributeId,
           NameStr(attributeP->attname),
           value != NULL ? " = \"" : "",
           value != NULL ? value : "",
           value != NULL ? "\"" : "",
           (unsigned int) (attributeP->atttypid),
           attributeP->attlen,
           attributeP->atttypmod,
           attributeP->attbyval ? 't' : 'f');
}
```