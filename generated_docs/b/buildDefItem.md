# buildDefItem

## Location
[src/backend/commands/tsearchcmds.c:1834-1870](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L1834-L1870)

## Overview
buildDefItem is a static function that constructs a DefElem node from parsed key-value parameters during text search configuration deserialization, with intelligent type detection and conversion.

## Definition

```c
static DefElem *
buildDefItem(const char *name, const char *val, bool was_quoted)
```
## Detailed Description
This function creates a DefElem (Definition Element) structure from a name-value pair extracted during parameter parsing. It performs intelligent type detection by attempting to parse unquoted values as integers, floats, or booleans before falling back to string representation. Quoted values are always treated as strings to preserve their exact textual representation. This function is essential for reconstructing structured configuration data from serialized text formats.

## Parameters / Member Variables
- `*name`: The parameter name/key as a null-terminated string
- `*val`: The parameter value as a null-terminated string
- `was_quoted`: Boolean flag indicating whether the original value was enclosed in quotes
## Dependencies
- Functions called/Symbols referenced:
  - [strtoint](../s/strtoint.md)
  - [makeDefElem](../m/makeDefElem.md)
  - [makeInteger](../m/makeInteger.md)
  - [makeFloat](../m/makeFloat.md)
  - [makeBoolean](../m/makeBoolean.md)
  - [makeString](../m/makeString.md)
  - [pstrdup](../p/pstrdup.md)
  - strtod
  - strcmp
- Called from (representative examples):
  - [ds_state](../d/ds_state.md) (multiple calls during parameter parsing)
  - TSTokenTypeItem

## Notes and Other Information
Located at src/backend/commands/tsearchcmds.c:1834-1870. The function uses a hierarchical type detection approach: first attempting integer parsing with strtoint(), then float parsing with strtod(), then boolean literal matching ("true"/"false"), and finally defaulting to string representation. The was_quoted parameter ensures that explicitly quoted values maintain their string type regardless of content, preserving user intent and preventing unintended type conversions. All string values are duplicated using pstrdup() to ensure proper memory management within PostgreSQL's memory contexts.

## Simplified Source

```c
static DefElem *buildDefItem(const char *name, const char *val, bool was_quoted)
{
    // If input was quoted, always treat as string
    if (!was_quoted && val[0] != '\0') {
        int v;
        char *endptr;

        // Try parsing as integer
        errno = 0;
        v = strtoint(val, &endptr, 10);
        if (errno == 0 && *endptr == '\0')
            return makeDefElem(pstrdup(name), (Node *) makeInteger(v), -1);

        // Try parsing as float
        errno = 0;
        (void) strtod(val, &endptr);
        if (errno == 0 && *endptr == '\0')
            return makeDefElem(pstrdup(name), (Node *) makeFloat(pstrdup(val)), -1);

        // Check for boolean literals
        if (strcmp(val, "true") == 0)
            return makeDefElem(pstrdup(name), (Node *) makeBoolean(true), -1);
        if (strcmp(val, "false") == 0)
            return makeDefElem(pstrdup(name), (Node *) makeBoolean(false), -1);
    }

    // Default to string type
    return makeDefElem(pstrdup(name), (Node *) makeString(pstrdup(val)), -1);
}
```