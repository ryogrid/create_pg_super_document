# parseNameAndArgTypes

## Location
[src/backend/utils/adt/regproc.c:1895-2038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1895-L2038)

## Overview
A complex parsing function that extracts a qualified function or operator name and its argument type list from a formatted string, converting the input into structured data for PostgreSQL's regtype system.

## Definition

```c
static bool
parseNameAndArgTypes(const char *string, bool allowNone, List **names,
					 int *nargs, Oid *argtypes,
					 Node *escontext)
```
## Detailed Description
The  function is a sophisticated parser that handles the complex task of decomposing function or operator signatures from their string representations. It expects input in the format "name(type1, type2, ...)", where the name can be schema-qualified and types can include complex constructs with parentheses and brackets.

The function performs several key operations:
1. Parses the qualified name portion before the opening parenthesis
2. Validates proper parentheses structure
3. Tokenizes the comma-separated argument type list
4. Handles quoted identifiers and nested parentheses/brackets within type specifications
5. Resolves each type name to its corresponding OID using the type parser
6. Supports a special "NONE" keyword for unary operators when allowNone is true

The parser is robust enough to handle PostgreSQL's full type syntax, including array types, complex types, and schema-qualified type names while maintaining proper quote and parentheses balancing.

## Parameters / Member Variables
- `*string`: Input string containing the function/operator signature to parse (format: "name(type1, type2, ...)")
- `allowNone`: Boolean flag that allows "NONE" as a valid type name, mapping it to InvalidOid (used for unary operators)
- `**names`: Output parameter - pointer to a List of Strings representing the parsed qualified name components
- `*nargs`: Output parameter - pointer to integer that will contain the number of parsed arguments
- `*argtypes`: Output parameter - array of Oids (size FUNC_MAX_ARGS) that will contain the resolved type OIDs
- `*escontext`: Error context node for soft error handling, enabling graceful error capture instead of exceptions
## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](pstrdup.md) (string duplication)
  - [stringToQualifiedNameList](../s/stringToQualifiedNameList.md) (qualified name parsing)
  - [scanner_isspace](../s/scanner_isspace.md) (whitespace detection)
  - [parseTypeString](parseTypeString.md) (type name resolution)
  - [pg_strcasecmp](pg_strcasecmp.md) (case-insensitive string comparison)
  - ereturn (soft error return macro)
  - FUNC_MAX_ARGS (maximum function arguments constant)
  - InvalidOid (PostgreSQL constant)
- Called from (representative examples):
  - [regprocedurein](../r/regprocedurein.md) (procedure input function)
  - [regoperatorin](../r/regoperatorin.md) (operator input function)

## Notes and Other Information
- This function is static and only accessible within regproc.c
- Supports complex PostgreSQL type syntax including arrays (e.g., "int[]") and composite types
- Properly handles quoted identifiers in both names and type specifications
- Enforces PostgreSQL's FUNC_MAX_ARGS limit on the number of function arguments
- The parser maintains state for quote and parentheses nesting to correctly identify commas that separate arguments
- Essential for parsing regprocedure and regoperator input values in PostgreSQL's type system
- Returns false only when escontext allows soft error handling; otherwise throws exceptions on parse errors
- Memory allocated for rawname is properly freed before function return

## Simplified Source

```c
static bool parseNameAndArgTypes(const char *string, bool allowNone, List **names,
                                int *nargs, Oid *argtypes, Node *escontext) {
    char *rawname = pstrdup(string);
    char *ptr;
    bool in_quote = false;

    // Find opening parenthesis (not in quotes)
    for (ptr = rawname; *ptr; ptr++) {
        if (*ptr == '"') in_quote = !in_quote;
        else if (*ptr == '(' && !in_quote) break;
    }
    if (*ptr == '\0') {
        return ereturn(escontext, false, /* missing left paren error */);
    }

    // Split name and argument list
    *ptr++ = '\0';
    *names = stringToQualifiedNameList(rawname, escontext);
    if (*names == NIL) return false;

    // Find and validate closing parenthesis
    char *ptr2 = ptr + strlen(ptr) - 1;
    while (ptr2 > ptr && scanner_isspace(*ptr2)) ptr2--;
    if (*ptr2 != ')') {
        return ereturn(escontext, false, /* missing right paren error */);
    }
    *ptr2 = '\0';

    // Parse comma-separated argument types
    *nargs = 0;
    bool had_comma = false;

    while (*ptr) {
        // Skip leading whitespace
        while (scanner_isspace(*ptr)) ptr++;
        if (*ptr == '\0') {
            if (had_comma) {
                return ereturn(escontext, false, /* expected type name error */);
            }
            break;
        }

        // Find end of type name (handle quotes and parentheses)
        char *typename = ptr;
        in_quote = false;
        int paren_count = 0;

        for (; *ptr; ptr++) {
            if (*ptr == '"') in_quote = !in_quote;
            else if (*ptr == ',' && !in_quote && paren_count == 0) break;
            else if (!in_quote) {
                if (*ptr == '(' || *ptr == '[') paren_count++;
                else if (*ptr == ')' || *ptr == ']') paren_count--;
            }
        }

        // Handle comma or end of string
        if (*ptr == ',') {
            had_comma = true;
            *ptr++ = '\0';
        } else {
            had_comma = false;
        }

        // Trim trailing whitespace from typename
        ptr2 = ptr - 1;
        while (ptr2 >= typename && scanner_isspace(*ptr2)) *ptr2-- = '\0';

        // Parse type name to OID
        Oid typeid;
        int32 typmod;
        if (allowNone && pg_strcasecmp(typename, "none") == 0) {
            typeid = InvalidOid;
        } else {
            if (!parseTypeString(typename, &typeid, &typmod, escontext))
                return false;
        }

        if (*nargs >= FUNC_MAX_ARGS) {
            return ereturn(escontext, false, /* too many arguments error */);
        }

        argtypes[*nargs] = typeid;
        (*nargs)++;
    }

    pfree(rawname);
    return true;
}
```