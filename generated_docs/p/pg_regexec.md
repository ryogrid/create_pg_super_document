# pg_regexec

## Location
src/backend/regex/regexec.c: 185 - 200

## Overview
pg_regexec is the main entry point for executing regular expression matches against text strings in PostgreSQL, providing comprehensive pattern matching with support for subexpression capture and advanced regex features.

## Definition

```c
struct vars var;
```
## Detailed Description
pg_regexec performs regular expression matching by executing a compiled regular expression pattern against a target string. The function supports both simple matching and complex operations including backreference handling, subexpression capture, and lookahead/lookbehind constraints.

The function operates in two main modes:
1. **Simple matching mode**: Uses DFA-based matching for patterns without backreferences
2. **Backref matching mode**: Uses more complex algorithms (cfind) for patterns containing backreferences

Key implementation details:
- Validates input parameters including regex magic number and character size compatibility
- Sets up locale-dependent collation support via pg_set_regex_collation()
- Allocates working memory for match results, using local stack arrays when possible for performance
- Constructs DFA (Deterministic Finite Automaton) structures for subexpressions and lookaround constraints
- Performs the actual matching using either find() or cfind() depending on pattern complexity
- Handles cleanup of all allocated resources including DFAs and match arrays

The function includes comprehensive error handling and returns standard POSIX regex error codes. It supports advanced PostgreSQL-specific features like collation awareness and detailed match reporting.

## Parameters / Member Variables
- : Compiled regular expression structure containing the pattern, flags, and internal automaton data
- : Input string to search for pattern matches (as chr* for Unicode support)  
- : Length of the input string in characters
- : Character offset within string where matching should begin (0 for start of string)
- : Optional structure for extended match information (required if REG_EXPECT flag is set)
- : Size of the pmatch array specifying how many subexpression matches to capture
- : Output array to store match positions for the overall match and subexpressions
- : Execution flags controlling matching behavior (e.g., REG_NOTBOL, REG_NOTEOL)

## Dependencies
- Functions called/Symbols referenced:
  - pg_set_regex_collation
  - zapallsubs  
  - find
  - cfind
  - freedfa
  - MALLOC/FREE (memory management macros)
- Called from (representative examples):
  - regexec_auth_token (in src/backend/libpq/hba.c:358)
  - CheckAffix (in src/backend/tsearch/spell.c:2148)
  - RE_wchar_execute (in src/backend/utils/adt/regexp.c:289)
  - replace_text_regexp (in src/backend/utils/adt/varlena.c:4254)
  - test_re_execute (in src/test/modules/test_regex/test_regex.c:221)

## Notes and Other Information
- Returns REG_OKAY (0) on successful match, REG_NOMATCH if no match found, or other REG_* error codes
- Uses local stack arrays (LOCALMAT=20, LOCALDFAS=40) to avoid memory allocation for small patterns
- Supports PostgreSQL's extended regex features beyond standard POSIX including lookahead/lookbehind
- Automatically handles character encoding compatibility checks between regex and input string
- The function is thread-safe as it uses local variables and doesn't modify the compiled regex structure
- Memory allocation failures return REG_ESPACE error code
- Includes debug trace support when compiled with REG_DEBUG flag
- Performance is optimized by reusing DFA structures within a single execution context