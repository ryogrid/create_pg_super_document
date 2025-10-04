# lexdigits

## Location
[src/backend/regex/regc_lex.c:780-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_lex.c#L780-L860)

## Overview
Parses a sequence of digits in a specified base (octal, decimal, or hexadecimal) and returns the accumulated numeric value as a character code.

## Definition
```c
static chr lexdigits(struct vars *v, int base, int minlen, int maxlen)
```

## Detailed Description
The `lexdigits` function is a utility function used to parse numeric escape sequences in regular expressions. It reads consecutive digit characters from the input stream and accumulates their value according to the specified base system. The function supports bases 8 (octal), 10 (decimal), and 16 (hexadecimal).

The function processes characters by:
1. **Digit recognition**: Recognizes `0-9` for all bases, plus `a-f`/`A-F` for hexadecimal
2. **Base validation**: Ensures each digit is valid for the specified base (e.g., `8` and `9` are invalid in octal)
3. **Accumulation**: Multiplies the current value by the base and adds the new digit value
4. **Length validation**: Ensures the parsed sequence meets minimum length requirements
5. **Bounds checking**: Stops parsing when the maximum length is reached

This function is primarily used for parsing:
- Unicode escape sequences (`\u1234`, `\U12345678`)
- Hexadecimal escapes (`\x1a`)
- Octal escapes (`\123`)
- Backreference numbers (`\1`, `\12`)

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing the input state and current position (`now`)
- `base`: Numeric base for parsing (8 for octal, 10 for decimal, 16 for hexadecimal)
- `minlen`: Minimum number of digits required (generates REG_EESCAPE error if not met)
- `maxlen`: Maximum number of digits to parse (parsing stops when reached)

## Dependencies
- Functions called/Symbols referenced:
  - ATEOS, CHR, DIGITVAL, ERR
- Constants referenced:
  - Character literals: CHR(0) through CHR(9), CHR(a) through CHR(f), CHR(A) through CHR(F)
  - Error codes: REG_EESCAPE
- Types used:
  - [chr](../c/chr.md), uchr (character types)
- Called from (representative examples):
  - [lexescape](lexescape.md) (for `\u`, `\U`, `\x`, octal, and backref parsing)

## Notes and Other Information
The function does not perform overflow checking, so callers must validate the result if the maxlen parameter could cause overflow. The function automatically backtracks when it encounters invalid digits or reaches the maximum length. For hexadecimal parsing, both uppercase and lowercase letters are accepted. The function uses unsigned arithmetic (uchr) internally to avoid undefined behavior on overflow, then casts the result back to chr. Invalid digits cause the parser to back up and terminate parsing, which allows for proper handling of partial numeric sequences.

## Simplified Source

```c
static chr
lexdigits(struct vars *v, int base, int minlen, int maxlen)
{
    uchr n = 0;
    int len = 0;
    const uchr ub = (uchr) base;

    // Parse digits up to maxlen characters
    while (len < maxlen && !ATEOS()) {
        chr c = *v->now++;
        int d = -1;

        // Convert character to digit value
        if (c >= '0' && c <= '9') {
            d = DIGITVAL(c);
        } else if (c >= 'a' && c <= 'f') {
            d = c - 'a' + 10;
        } else if (c >= 'A' && c <= 'F') {
            d = c - 'A' + 10;
        } else {
            // Not a valid digit character
            v->now--;
            break;
        }

        // Check if digit is valid for this base
        if (d >= base) {
            v->now--;
            break;
        }

        // Accumulate the digit value
        n = n * ub + (uchr) d;
        len++;
    }

    // Check minimum length requirement
    if (len < minlen) {
        ERR(REG_EESCAPE);
    }

    return (chr) n;
}
```