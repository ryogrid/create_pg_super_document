# fns

## Location
[src/include/regex/regguts.h:517-522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/regex/regguts.h#L517-L522)

## Overview
The  struct defines a function pointer table for generic regex manipulation functions, providing a pluggable interface for regex operations in PostgreSQL's regex engine.

## Definition

Where  expands to , making the actual definition:


## Detailed Description
The  struct serves as a vtable (virtual function table) for regex operations, allowing different implementations of core regex functionality to be plugged in at runtime. Each  object contains a  pointer that references one of these function tables. This design provides flexibility for different regex backends or specialized implementations while maintaining a consistent interface. The structure currently defines two essential operations: memory cleanup and stack depth checking for preventing infinite recursion.

## Parameters / Member Variables
-                total        used        free      shared  buff/cache   available
Mem:        32819380     5371292    23832320        3060     3615768    27065900
Swap:        8388608           0     8388608: Function pointer for deallocating regex resources
  - Takes a  parameter pointing to the regex object to be freed
  - Returns void
  - Responsible for cleaning up all memory associated with the regex
- : Function pointer for checking recursion depth
  - Takes no parameters (void)
  - Returns int (typically non-zero if stack is too deep, zero otherwise)
  - Used to prevent stack overflow during regex processing

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL regex type, aliased to )
  -  macro (expands to function pointer syntax)
- Called from (representative examples):
  -  macro (regex compilation)
  -  (regex cleanup)
  -  macro (stack depth checking)

## Notes and Other Information
- The  macro provides convenient access: 
- This function table pattern allows PostgreSQL to maintain compatibility with different regex engine implementations
- The design supports future extensibility by allowing additional function pointers to be added to the structure
- Stack depth checking is crucial for preventing infinite recursion in complex regex patterns
- The function table is typically statically allocated and shared among regex objects with the same implementation