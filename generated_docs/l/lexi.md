# lexi

## Location
[src/tools/pg_bsd_indent/lexi.c:216-676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/lexi.c#L216-L676)

## Overview
The main lexical analyzer function for pg_bsd_indent that tokenizes C source code and returns appropriate token codes for the parser.

## Definition
int lexi(struct parser_state *state)

## Detailed Description
This is the central lexical analysis function that scans the input buffer and identifies tokens in C source code. It handles alphanumeric tokens (identifiers, keywords, numbers), string and character literals, operators, punctuation, and comments. The function maintains parser state information and determines whether identifiers are keywords, type names, or regular identifiers. It also performs lookahead analysis to distinguish between function definitions and declarations. The function returns integer token codes that guide the parsing and formatting process.

## Parameters / Member Variables
- state: Pointer to parser_state structure containing current parsing context, column position, keyword information, and various flags

## Dependencies
- Functions called/Symbols referenced:
  - [fill_buffer](../f/fill_buffer.md) (refills input buffer when needed)
  - [strcmp_type](../s/strcmp_type.md) (comparison function for binary search)
  - [is_func_definition](../i/is_func_definition.md) (determines if parentheses indicate function definition)
  - [diag2](../d/diag2.md) (diagnostic message output)
  - CHECK_SIZE_TOKEN (macro to ensure token buffer space)
- Called from (representative examples):
  - [main](../m/main.md) (at src/tools/pg_bsd_indent/indent.c:269)
  - [main](../m/main.md) (at src/tools/pg_bsd_indent/indent.c:448)

## Notes and Other Information
- Returns various token type codes: ident, decl, funcname, comment, newline, etc.
- Handles C numeric literals including binary (0b), octal (0), hexadecimal (0x), and decimal formats
- Recognizes C keywords through binary search in specials table
- Supports type name recognition through typename tables and _t suffix heuristics
- Manages string and character literal parsing with escape sequence handling  
- Tracks unary vs binary operator context through parser state
- Handles comment detection and parsing (both /* */ and // styles)
- Maintains column position information for formatting decisions
- Core component of the pg_bsd_indent code formatting tool

## Simplified Source

```c
int lexi(struct parser_state *state) {
    int unary_delim = false;
    int code;
    char qchar;

    // Set up token pointers and state
    e_token = s_token;
    state->col_1 = state->last_nl;
    state->last_nl = false;

    // Skip whitespace
    while (*buf_ptr == ' ' || *buf_ptr == '\t') {
        state->col_1 = false;
        if (++buf_ptr >= buf_end) fill_buffer();
    }

    // Handle alphanumeric tokens (identifiers, numbers, keywords)
    if (chartype[*buf_ptr & 127] == alphanum ||
        (buf_ptr[0] == '.' && isdigit(buf_ptr[1]))) {

        // Parse numeric literals
        if (isdigit(*buf_ptr) || (buf_ptr[0] == '.' && isdigit(buf_ptr[1]))) {
            // Handle binary (0b), hex (0x), octal (0), and decimal numbers
            if (buf_ptr[0] == '0' && buf_ptr[1] != '.') {
                // Parse binary, hex, or octal
                int len = parse_special_number_base();
                copy_token(len);
            } else {
                // Parse decimal number with possible decimal point and exponent
                parse_decimal_number();
            }
            // Handle numeric suffixes (U, L, f, etc.)
            parse_numeric_suffixes();
        } else {
            // Parse identifier/keyword
            while (chartype[*buf_ptr & 127] == alphanum || *buf_ptr == BACKSLASH) {
                if (*buf_ptr == BACKSLASH && *(buf_ptr + 1) == '\n') {
                    buf_ptr += 2;  // Skip escaped newline
                    if (buf_ptr >= buf_end) fill_buffer();
                } else {
                    *e_token++ = *buf_ptr++;
                    if (buf_ptr >= buf_end) fill_buffer();
                }
            }
        }
        *e_token = '\0';

        // Handle special prefix cases (L"string" or L'char')
        if (s_token[0] == 'L' && s_token[1] == '\0' &&
            (*buf_ptr == '"' || *buf_ptr == '\'')) {
            return strpfx;
        }

        // Skip trailing whitespace
        while (*buf_ptr == ' ' || *buf_ptr == '\t') {
            if (++buf_ptr >= buf_end) fill_buffer();
        }

        // Determine token type: keyword, type, or identifier
        state->keyword = 0;
        if (state->last_token == structure && !state->p_l_follow) {
            state->last_u_d = true;
            return decl;
        }

        // Look up in keywords table
        struct templ *p = bsearch(s_token, specials,
                                  sizeof(specials)/sizeof(specials[0]),
                                  sizeof(specials[0]), strcmp_type);

        if (p == NULL) {
            // Check if it's a type name (_t suffix or in typenames table)
            if (is_typename(s_token)) {
                state->keyword = 4;
                state->last_u_d = true;
                return handle_typename();
            }
        } else {
            // Handle keywords
            state->keyword = p->rwcode;
            state->last_u_d = true;
            return handle_keyword(p->rwcode);
        }

        // Check for function definition
        if (*buf_ptr == '(' && is_function_context() &&
            is_func_definition(buf_ptr)) {
            strncpy(state->procname, token, sizeof(state->procname) - 1);
            if (state->in_decl) state->in_parameter_declaration = 1;
            return funcname;
        }

        // Additional type detection heuristics
        if (looks_like_declaration()) {
            state->keyword = 4;
            state->last_u_d = true;
            return decl;
        }

        return ident;
    }

    // Handle non-alphanumeric tokens
    *e_token++ = *buf_ptr;
    *e_token = '\0';
    if (++buf_ptr >= buf_end) fill_buffer();

    switch (*token) {
    case '\n':
        unary_delim = state->last_u_d;
        state->last_nl = true;
        code = (had_eof ? 0 : newline);
        break;

    case '\'':  // Character literal
    case '"':   // String literal
        qchar = *token;
        do {
            while (1) {
                if (*buf_ptr == '\n') {
                    diag2(1, "Unterminated literal");
                    goto stop_lit;
                }
                *e_token = *buf_ptr++;
                if (buf_ptr >= buf_end) fill_buffer();

                if (*e_token == BACKSLASH) {
                    // Handle escape sequences
                    if (*buf_ptr == '\n') ++line_no;
                    *++e_token = *buf_ptr++;
                    ++e_token;
                    if (buf_ptr >= buf_end) fill_buffer();
                } else {
                    break;
                }
            }
        } while (*e_token++ != qchar);
stop_lit:
        code = ident;
        break;

    case '(':
    case '[':
        unary_delim = true;
        code = lparen;
        break;

    case ')':
    case ']':
        code = rparen;
        break;

    case '{':
        unary_delim = true;
        code = lbrace;
        break;

    case '}':
        unary_delim = true;
        code = rbrace;
        break;

    case ';':
        unary_delim = true;
        code = semicolon;
        break;

    case ',':
        unary_delim = true;
        code = comma;
        break;

    case '.':
        unary_delim = false;
        code = period;
        break;

    case '-':
    case '+':
        code = (state->last_u_d ? unary_op : binary_op);
        unary_delim = true;
        handle_plus_minus_operators();
        break;

    case '=':
        if (state->in_or_st) state->block_init = 1;
        if (*buf_ptr == '=') {
            *e_token++ = '=';
            buf_ptr++;
        }
        code = binary_op;
        unary_delim = true;
        break;

    case '>':
    case '<':
    case '!':
        handle_comparison_operators();
        code = (state->last_u_d ? unary_op : binary_op);
        unary_delim = true;
        break;

    case '*':
        unary_delim = true;
        if (!state->last_u_d) {
            if (*buf_ptr == '=') *e_token++ = *buf_ptr++;
            code = binary_op;
        } else {
            handle_pointer_operators();
            code = unary_op;
        }
        break;

    default:
        if (token[0] == '/' && *buf_ptr == '*') {
            // Comment start
            *e_token++ = '*';
            if (++buf_ptr >= buf_end) fill_buffer();
            code = comment;
            unary_delim = state->last_u_d;
        } else {
            // Handle other operators (||, &&, etc.)
            handle_other_operators();
            code = (state->last_u_d ? unary_op : binary_op);
            unary_delim = true;
        }
        break;
    }

    // Cleanup and return
    if (buf_ptr >= buf_end) fill_buffer();
    state->last_u_d = unary_delim;
    *e_token = '\0';
    return code;
}
```