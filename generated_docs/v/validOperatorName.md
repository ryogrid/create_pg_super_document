# validOperatorName

## Location
[src/backend/catalog/pg_operator.c:58-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_operator.c#L58-L112)

## Overview
Validates whether a proposed operator name conforms to PostgreSQL's operator naming rules and lexical constraints.

## Definition

```c
static bool
validOperatorName(const char *name)
```

## Detailed Description
This function ensures that operator names comply with PostgreSQL's lexical rules, which must match the behavior of the parser/scanner. It performs multiple validation checks including length constraints, character restrictions, comment sequence prevention, and SQL standard compatibility rules. The function is essential for preventing lexical ambiguities and ensuring that custom operators don't conflict with existing syntax.

## Parameters / Member Variables
- `name`: C string containing the proposed operator name to validate

## Dependencies
- Functions called/Symbols referenced:
  - strlen: Standard C library function for string length
  - strspn: Standard C library function for character span validation
  - strstr: Standard C library function for substring search
  - strchr: Standard C library function for character search
  - strcmp: Standard C library function for string comparison

## Notes and Other Information
- Must match the behavior of parser/scan.l for consistency
- Validates against valid operator characters: "~!@#^&|`?+-*/%<>="
- Prevents comment sequences "/*" and "--" in operator names
- Enforces SQL standard compatibility for multi-character operators ending with '+' or '-'
- Specifically rejects "!=" as it's converted to "<>" by the parser
- Length must be between 1 and NAMEDATALEN-1 characters

## Simplified Source

```c
static bool
validOperatorName(const char *name)
{
    size_t len = strlen(name);

    // Check length constraints
    if (len == 0 || len >= NAMEDATALEN)
        return false;

    // Validate character set (must match scan.l op_chars)
    if (strspn(name, "~!@#^&|`?+-*/%<>=") != len)
        return false;

    // Prevent comment sequences
    if (strstr(name, "/*") || strstr(name, "--"))
        return false;

    // SQL standard compatibility for operators ending with +/-
    if (len > 1 && (name[len - 1] == '+' || name[len - 1] == '-')) {
        // Must contain non-SQL operator chars to be valid
        for (int ic = len - 2; ic >= 0; ic--) {
            if (strchr("~!@#^&|`?%", name[ic]))
                break;
        }
        if (ic < 0)
            return false; // Only SQL chars found, invalid
    }

    // Special case: != is not valid (converted to <> by parser)
    if (strcmp(name, "!=") == 0)
        return false;

    return true;
}
```