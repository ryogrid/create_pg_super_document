# replace_domain_constraint_value

## Location
[src/backend/commands/typecmds.c:3637-3667](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3637-L3667)

## Overview
A parser hook function that replaces references to "value" in domain check constraint expressions with appropriate CoerceToDomainValue nodes during expression parsing.

## Definition
```c
static Node *replace_domain_constraint_value(ParseState *pstate, ColumnRef *cref)
```

## Detailed Description
This function serves as a specialized parser hook that intercepts column reference resolution during domain constraint expression parsing. When the parser encounters a reference to "value" (case-sensitive), this function substitutes it with a CoerceToDomainValue node that represents the domain value being tested against the constraint.

The function is designed to handle VALUE as an identifier rather than a keyword, preserving backward compatibility with applications that may have used "value" as a column name. It only performs the substitution for simple, unqualified references to "value" and returns NULL for all other column references, allowing normal parser processing to continue.

The CoerceToDomainValue node is copied from the parser state's hook context (set up by domainAddCheckConstraint) and the location information is preserved for accurate error reporting.

## Parameters / Member Variables
- `pstate`: Parser state containing context information and the hook state
- `cref`: Column reference being resolved by the parser

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (check for single field reference)
  - linitial (get first list element)
  - strVal (extract string value from node)
  - strcmp (string comparison)
  - copyObject (create copy of CoerceToDomainValue)
- Called from:
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md) (set as p_pre_columnref_hook during parsing)

## Notes and Other Information
- Installed as p_pre_columnref_hook in ParseState during domain constraint parsing
- Only handles simple, unqualified references to "value" (not schema.table.value)
- Returns NULL for all non-"value" references to continue normal parsing
- Preserves source location information for accurate error reporting
- Case-sensitive matching for "value" identifier
- Part of the domain constraint expression parsing infrastructure
- Enables the special VALUE keyword functionality in domain check constraints
- Maintains compatibility by treating VALUE as an identifier, not a reserved keyword

## Simplified Source

```c
static Node *
replace_domain_constraint_value(ParseState *pstate, ColumnRef *cref)
{
    // Check for a reference to "value" and replace with CoerceToDomainValue
    // Handle VALUE as a name, not a keyword, for backward compatibility

    if (list_length(cref->fields) == 1)
    {
        Node *field1 = (Node *) linitial(cref->fields);
        char *colname = strVal(field1);

        if (strcmp(colname, "value") == 0)
        {
            // Replace "value" with prepared CoerceToDomainValue node
            CoerceToDomainValue *domVal = copyObject(pstate->p_ref_hook_state);

            // Preserve location for error reporting
            domVal->location = cref->location;
            return (Node *) domVal;
        }
    }

    // Return NULL for all other references to continue normal parsing
    return NULL;
}
```