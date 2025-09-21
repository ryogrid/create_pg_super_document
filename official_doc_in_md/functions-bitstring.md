9.6. Bit String Functions and Operators  
---  
[Prev](functions-binarystring.md "9.5. Binary String Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")| Chapter 9. Functions and Operators| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](functions-matching.md "9.7. Pattern Matching")  
  
* * *

## 9.6. Bit String Functions and Operators #

This section describes functions and operators for examining and manipulating bit strings, that is values of the types `bit` and `bit varying`. (While only type `bit` is mentioned in these tables, values of type `bit varying` can be used interchangeably.) Bit strings support the usual comparison operators shown in [Table 9.1](functions-comparison.md#FUNCTIONS-COMPARISON-OP-TABLE "Table 9.1. Comparison Operators"), as well as the operators shown in [Table 9.14](functions-bitstring.md#FUNCTIONS-BIT-STRING-OP-TABLE "Table 9.14. Bit String Operators"). 

**Table 9.14. Bit String Operators**

Operator  Description  Example(s)   
---  
`bit` `||` `bit` → `bit` Concatenation  `B'10001' || B'011'` → `10001011`  
`bit` `&` `bit` → `bit` Bitwise AND (inputs must be of equal length)  `B'10001' & B'01101'` → `00001`  
`bit` `|` `bit` → `bit` Bitwise OR (inputs must be of equal length)  `B'10001' | B'01101'` → `11101`  
`bit` `#` `bit` → `bit` Bitwise exclusive OR (inputs must be of equal length)  `B'10001' # B'01101'` → `11100`  
`~` `bit` → `bit` Bitwise NOT  `~ B'10001'` → `01110`  
`bit` `<<` `integer` → `bit` Bitwise shift left (string length is preserved)  `B'10001' << 3` → `01000`  
`bit` `>>` `integer` → `bit` Bitwise shift right (string length is preserved)  `B'10001' >> 2` → `00100`  
  
  


Some of the functions available for binary strings are also available for bit strings, as shown in [Table 9.15](functions-bitstring.md#FUNCTIONS-BIT-STRING-TABLE "Table 9.15. Bit String Functions"). 

**Table 9.15. Bit String Functions**

Function  Description  Example(s)   
---  
`bit_count` ( `bit` ) → `bigint` Returns the number of bits set in the bit string (also known as “popcount”).  `bit_count(B'10111')` → `4`  
`bit_length` ( `bit` ) → `integer` Returns number of bits in the bit string.  `bit_length(B'10111')` → `5`  
`length` ( `bit` ) → `integer` Returns number of bits in the bit string.  `length(B'10111')` → `5`  
`octet_length` ( `bit` ) → `integer` Returns number of bytes in the bit string.  `octet_length(B'1011111011')` → `2`  
`overlay` ( _`bits`_ `bit` `PLACING` _`newsubstring`_ `bit` `FROM` _`start`_ `integer` [ `FOR` _`count`_ `integer` ] ) → `bit` Replaces the substring of _`bits`_ that starts at the _`start`_ 'th bit and extends for _`count`_ bits with _`newsubstring`_. If _`count`_ is omitted, it defaults to the length of _`newsubstring`_.  `overlay(B'01010101010101010' placing B'11111' from 2 for 3)` → `0111110101010101010`  
`position` ( _`substring`_ `bit` `IN` _`bits`_ `bit` ) → `integer` Returns first starting index of the specified _`substring`_ within _`bits`_ , or zero if it's not present.  `position(B'010' in B'000001101011')` → `8`  
`substring` ( _`bits`_ `bit` [ `FROM` _`start`_ `integer` ] [ `FOR` _`count`_ `integer` ] ) → `bit` Extracts the substring of _`bits`_ starting at the _`start`_ 'th bit if that is specified, and stopping after _`count`_ bits if that is specified. Provide at least one of _`start`_ and _`count`_.  `substring(B'110010111111' from 3 for 2)` → `00`  
`get_bit` ( _`bits`_ `bit`, _`n`_ `integer` ) → `integer` Extracts _`n`_ 'th bit from bit string; the first (leftmost) bit is bit 0.  `get_bit(B'101010101010101010', 6)` → `1`  
`set_bit` ( _`bits`_ `bit`, _`n`_ `integer`, _`newvalue`_ `integer` ) → `bit` Sets _`n`_ 'th bit in bit string to _`newvalue`_ ; the first (leftmost) bit is bit 0.  `set_bit(B'101010101010101010', 6, 0)` → `101010001010101010`  
  
  


In addition, it is possible to cast integral values to and from type `bit`. Casting an integer to `bit(n)` copies the rightmost `n` bits. Casting an integer to a bit string width wider than the integer itself will sign-extend on the left. Some examples: 
    
    
    44::bit(10)                    _0000101100_
    44::bit(3)                     _100_
    cast(-44 as bit(12))           _111111010100_
    '1110'::bit(4)::integer        _14_
    

Note that casting to just “bit” means casting to `bit(1)`, and so will deliver only the least significant bit of the integer. 

* * *

[Prev](functions-binarystring.md "9.5. Binary String Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")|  [Next](functions-matching.md "9.7. Pattern Matching")  
---|---|---  
9.5. Binary String Functions and Operators | [Home](index.md "PostgreSQL 17.5 Documentation")|  9.7. Pattern Matching
