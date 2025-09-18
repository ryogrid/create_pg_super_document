# lexdigits

## Location
src/backend/regex/regc_lex.c: 780 - 860

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