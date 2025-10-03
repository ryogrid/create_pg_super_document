# DPPP_dopoptosub_at

## Location
[src/pl/plperl/ppport.h:16145-16173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/ppport.h#L16145-L16173)

## Overview
A static function that searches backwards through the Perl context stack to find the nearest subroutine, eval, or format context, used for stack unwinding operations.

## Definition

```c
static I32
DPPP_dopoptosub_at(const PERL_CONTEXT *cxstk, I32 startingblock)
```
## Detailed Description
The  function implements a backward search through Perl's context stack to locate the most recent context of specific types. It examines each context starting from the given  index and moving toward the stack bottom (index 0). The function specifically looks for three types of contexts that represent callable or evaluable code blocks:

- **CXt_SUB**: Subroutine contexts (regular Perl subroutines)
- **CXt_EVAL**: Eval contexts (string eval or block eval)  
- **CXt_FORMAT**: Format contexts (Perl format blocks)

This functionality is essential for implementing caller() and other introspection functions that need to understand the call stack structure. The function returns the index of the first matching context found, or the final loop index if no matching context is found.

## Parameters / Member Variables
- `*cxstk`: Pointer to the Perl context stack array (const PERL_CONTEXT *)
- `startingblock`: Starting index in the context stack to begin the backward search (I32)
## Dependencies
- Functions called/Symbols referenced:
  - CxTYPE (macro to get context type)
  - PERL_CONTEXT (context structure type)
  - CXt_EVAL, CXt_SUB, CXt_FORMAT (context type constants)

- Called from (representative examples):
  - Perl_caller_cx (multiple references in ppport.h for implementing caller() functionality)
  - caller_cx (context introspection functions)

## Notes and Other Information
- This function is defined in ppport.h starting at line 16144
- Part of the Devel::PPPort compatibility layer providing consistent caller() behavior across Perl versions
- The 'dopopt' prefix follows Perl internal naming conventions (do + pop + to = "do pop to")
- Returns the stack index rather than the context itself, allowing callers to access both the context and its position
- Critical for proper exception handling and call stack introspection in Perl XS code
- The backward iteration (i--) reflects the stack growth direction in Perl's context management
- Used primarily by caller_cx implementations to provide accurate caller information regardless of Perl version