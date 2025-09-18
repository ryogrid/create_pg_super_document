# utf8_advance

## Location
src/common/wchar.c: 1873 - 1890

## Overview
Advances the UTF-8 state machine by processing a sequence of bytes, used as part of PostgreSQL's optimized UTF-8 validation algorithm.

## Definition


## Detailed Description
This function implements a core component of PostgreSQL's shift-based deterministic finite automaton (DFA) for UTF-8 validation. It processes a sequence of bytes by advancing through the UTF-8 state machine using a packed 32-bit transition table. The function deliberately does not validate the input state value, allowing it to be used in optimized validation routines where state checking is handled elsewhere.

The implementation uses a carefully designed state transition table (Utf8Transition) that encodes all possible UTF-8 state transitions in 32-bit integers. The state values are specifically chosen to fit within a 32-bit packed representation, making the validation process highly efficient. The function applies a bitwise mask of 31 to ensure shifts operate correctly across different instruction sets where 32-bit shifts are treated as modulo 32 operations.

## Parameters / Member Variables
- : Pointer to the sequence of bytes to process
- : Pointer to the current UTF-8 validation state, modified in-place as bytes are processed
- : Number of bytes to process from the input sequence

## Dependencies
- Functions called/Symbols referenced:
  - Utf8Transition (static transition table)
- Called from (representative examples):
  - Used within STRIDE_LENGTH macro context for vectorized UTF-8 validation

## Notes and Other Information
- The mask value 31 is critical for compiler optimization - it allows the compiler to elide the mask operation on most instruction sets since 32-bit shifts are inherently modulo 32
- The function intentionally skips state validation for performance reasons, trusting the caller to provide valid states
- Part of PostgreSQL's optimized UTF-8 validation system that supports both byte-wise and vectorized processing
- The state encoding uses a sophisticated packed representation discovered through SMT solver optimization, allowing 64-bit state transitions to fit in 32-bit integers
- Final state masking ensures the state remains within valid bounds after processing