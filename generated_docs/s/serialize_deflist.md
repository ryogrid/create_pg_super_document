# serialize_deflist

## Location
[src/backend/commands/tsearchcmds.c:1565-1620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L1565-L1620)

## Overview
A utility function that converts a list of DefElem structures into a formatted TEXT datum suitable for storage in pg_ts_dict.dictinitoption, formatted exactly as needed for CREATE TEXT SEARCH DICTIONARY commands.

## Definition

```c
struction of the node type as well as the value.
		 */
		if (IsA(defel->arg, Integer) || IsA(defel->arg, Float))
			appendStringInfoString(&buf, val);
```
## Detailed Description
This function transforms a PostgreSQL List of DefElem structures into a properly formatted text string that represents dictionary options. The output format is designed to be pg_dump-compatible, meaning it produces text that could be directly used in a CREATE TEXT SEARCH DICTIONARY statement to reproduce the same configuration. The function handles different data types appropriately: numeric values (Integer/Float) are emitted without quotes, while string values are properly quoted with SQL escaping. Special attention is given to backslash handling using escape string syntax when necessary. Each option is formatted as 'name = value' with proper identifier quoting and comma separation between multiple options.

## Parameters / Member Variables
- : A PostgreSQL List containing DefElem structures, each representing a dictionary configuration option with a name and value

## Dependencies
- Functions called/Symbols referenced:
  - [DefElem](../D/DefElem.md) (structure type)
  - [defGetString](../d/defGetString.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [Integer](../I/Integer.md), Float (node types)
  - ESCAPE_STRING_SYNTAX
  - SQL_STR_DOUBLE
  - [lnext](../l/lnext.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - [initStringInfo](../i/initStringInfo.md), appendStringInfo, appendStringInfoString, appendStringInfoChar
- Called from (representative examples):
  - [DefineTSDictionary](../D/DefineTSDictionary.md)
  - [AlterTSDictionary](../A/AlterTSDictionary.md)

## Notes and Other Information
- Returns a TEXT datum that can be stored directly in PostgreSQL catalog tables
- Output format is specifically designed for pg_dump compatibility
- Handles proper SQL string escaping including backslash doubling and quote escaping
- Uses PostgreSQL's StringInfo buffer for efficient string building
- Automatically detects numeric vs string values and applies appropriate formatting
- Memory management includes proper cleanup of the StringInfo buffer
- Part of PostgreSQL's text search dictionary management system
- The function is declared in defrem.h, making it available to other subsystems
- Produces human-readable output that matches SQL syntax conventions

## Simplified Source

```c
text *
serialize_deflist(List *deflist)
{
    text *result;
    StringInfoData buf;
    ListCell *l;

    initStringInfo(&buf);

    // Process each DefElem in the list
    foreach(l, deflist)
    {
        DefElem *defel = (DefElem *) lfirst(l);
        char *val = defGetString(defel);

        // Format as "name = value"
        appendStringInfo(&buf, "%s = ", quote_identifier(defel->defname));

        // Handle numeric vs string values differently
        if (IsA(defel->arg, Integer) || IsA(defel->arg, Float))
        {
            // Numeric values: no quotes needed
            appendStringInfoString(&buf, val);
        }
        else
        {
            // String values: add quotes and proper escaping
            if (strchr(val, '\\'))
                appendStringInfoChar(&buf, ESCAPE_STRING_SYNTAX);

            appendStringInfoChar(&buf, '\'');
            while (*val)
            {
                char ch = *val++;

                // Double quotes and backslashes for SQL escaping
                if (SQL_STR_DOUBLE(ch, true))
                    appendStringInfoChar(&buf, ch);
                appendStringInfoChar(&buf, ch);
            }
            appendStringInfoChar(&buf, '\'');
        }

        // Add comma separator between options (except for last one)
        if (lnext(deflist, l) != NULL)
            appendStringInfoString(&buf, ", ");
    }

    // Convert to TEXT datum and clean up
    result = cstring_to_text_with_len(buf.data, buf.len);
    pfree(buf.data);
    return result;
}
```