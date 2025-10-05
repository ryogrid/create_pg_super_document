# RS_compile

## Location
[src/backend/tsearch/regis.c:85-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/regis.c#L85-L165)

## Overview
Compiles a regular expression string into a linked list of RegisNode structures for efficient pattern matching in PostgreSQL's text search system.

## Definition

```c
void
RS_compile(Regis *r, bool issuffix, const char *str)
```
## Detailed Description
RS_compile parses and compiles a simplified regular expression pattern into an internal representation using a finite state machine. The function processes the input string character by character, creating a linked list of RegisNode structures that represent the pattern components. It supports:
- Individual alphabetic characters
- Character classes [abc] (RSF_ONEOF type)
- Negated character classes [^abc] (RSF_NONEOF type)

The compiled pattern is stored in the Regis structure, which tracks whether it's a suffix pattern and maintains the count of characters in the pattern. The function must be kept synchronized with RS_isRegis for validation.

## Parameters / Member Variables
- `*r`: Pointer to Regis structure to store the compiled pattern
- `issuffix`: Boolean indicating whether this is a suffix pattern
- `*str`: The regular expression string to compile
## Dependencies
- Functions called/Symbols referenced:
  - [newRegisNode](../n/newRegisNode.md) (create new pattern nodes)
  - [t_isalpha](../t/t_isalpha.md) (check if character is alphabetic)
  - t_iseq (check character equality)
  - [pg_mblen](../p/pg_mblen.md) (get multibyte character length)
  - COPYCHAR (copy multibyte character)
  - memset (memory initialization)
  - strlen (string length)
  - elog (error logging)
- Types and constants:
  - [Regis](Regis.md) (main regex structure)
  - [RegisNode](RegisNode.md) (pattern node structure)
  - RSF_ONEOF, RSF_NONEOF (pattern type flags)
  - State constants: RS_IN_WAIT, RS_IN_ONEOF, RS_IN_ONEOF_IN, RS_IN_NONEOF
- Called from:
  - [NIAddAffix](../N/NIAddAffix.md) (in spell.c:710)

## Notes and Other Information
- Initializes the Regis structure with memset before compilation
- Maintains a character count (nchar) for the compiled pattern
- Uses multibyte-aware character processing for international text support
- Validates input patterns and throws errors for invalid syntax
- Must end in RS_IN_WAIT state for valid patterns
- Part of PostgreSQL's text search regex compilation infrastructure
- Works in tandem with RS_isRegis for pattern validation

## Simplified Source

```c
void RS_compile(Regis *r, bool issuffix, const char *str) {
    int len = strlen(str);
    int state = RS_IN_WAIT;
    const char *c = str;
    RegisNode *ptr = NULL;

    // Initialize regex structure
    memset(r, 0, sizeof(Regis));
    r->issuffix = (issuffix) ? 1 : 0;

    // Parse and compile pattern into linked list
    while (*c) {
        if (state == RS_IN_WAIT) {
            if (t_isalpha(c)) {
                // Create node for single character
                ptr = ptr ? newRegisNode(ptr, len) : (r->node = newRegisNode(NULL, len));
                COPYCHAR(ptr->data, c);
                ptr->type = RSF_ONEOF;
                ptr->len = pg_mblen(c);
            }
            else if (t_iseq(c, '[')) {
                // Start character class
                ptr = ptr ? newRegisNode(ptr, len) : (r->node = newRegisNode(NULL, len));
                ptr->type = RSF_ONEOF;
                state = RS_IN_ONEOF;
            }
            else
                elog(ERROR, "invalid regis pattern: \"%s\"", str);
        }
        else if (state == RS_IN_ONEOF) {
            if (t_iseq(c, '^')) {
                ptr->type = RSF_NONEOF;  // Negated character class
                state = RS_IN_NONEOF;
            }
            else if (t_isalpha(c)) {
                COPYCHAR(ptr->data, c);
                ptr->len = pg_mblen(c);
                state = RS_IN_ONEOF_IN;
            }
            else
                elog(ERROR, "invalid regis pattern: \"%s\"", str);
        }
        else if (state == RS_IN_ONEOF_IN || state == RS_IN_NONEOF) {
            if (t_isalpha(c)) {
                // Add more characters to class
                COPYCHAR(ptr->data + ptr->len, c);
                ptr->len += pg_mblen(c);
            }
            else if (t_iseq(c, ']'))
                state = RS_IN_WAIT;  // End character class
            else
                elog(ERROR, "invalid regis pattern: \"%s\"", str);
        }

        c += pg_mblen(c);
    }

    if (state != RS_IN_WAIT)
        elog(ERROR, "invalid regis pattern: \"%s\"", str);

    // Count total characters in pattern
    ptr = r->node;
    while (ptr) {
        r->nchar++;
        ptr = ptr->next;
    }
}
```