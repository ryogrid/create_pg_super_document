9.5. Binary String Functions and Operators  
---  
[Prev](functions-string.md "9.4. String Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")| Chapter 9. Functions and Operators| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](functions-bitstring.md "9.6. Bit String Functions and Operators")  
  
* * *

## 9.5. Binary String Functions and Operators #

This section describes functions and operators for examining and manipulating binary strings, that is values of type `bytea`. Many of these are equivalent, in purpose and syntax, to the text-string functions described in the previous section. 

SQL defines some string functions that use key words, rather than commas, to separate arguments. Details are in [Table 9.11](functions-binarystring.md#FUNCTIONS-BINARYSTRING-SQL "Table 9.11. SQL Binary String Functions and Operators"). PostgreSQL also provides versions of these functions that use the regular function invocation syntax (see [Table 9.12](functions-binarystring.md#FUNCTIONS-BINARYSTRING-OTHER "Table 9.12. Other Binary String Functions")). 

**Table 9.11. SQL Binary String Functions and Operators**

Function/Operator  Description  Example(s)   
---  
`bytea` `||` `bytea` → `bytea` Concatenates the two binary strings.  `'\x123456'::bytea || '\x789a00bcde'::bytea` → `\x123456789a00bcde`  
`bit_length` ( `bytea` ) → `integer` Returns number of bits in the binary string (8 times the `octet_length`).  `bit_length('\x123456'::bytea)` → `24`  
`btrim` ( _`bytes`_ `bytea`, _`bytesremoved`_ `bytea` ) → `bytea` Removes the longest string containing only bytes appearing in _`bytesremoved`_ from the start and end of _`bytes`_.  `btrim('\x1234567890'::bytea, '\x9012'::bytea)` → `\x345678`  
`ltrim` ( _`bytes`_ `bytea`, _`bytesremoved`_ `bytea` ) → `bytea` Removes the longest string containing only bytes appearing in _`bytesremoved`_ from the start of _`bytes`_.  `ltrim('\x1234567890'::bytea, '\x9012'::bytea)` → `\x34567890`  
`octet_length` ( `bytea` ) → `integer` Returns number of bytes in the binary string.  `octet_length('\x123456'::bytea)` → `3`  
`overlay` ( _`bytes`_ `bytea` `PLACING` _`newsubstring`_ `bytea` `FROM` _`start`_ `integer` [ `FOR` _`count`_ `integer` ] ) → `bytea` Replaces the substring of _`bytes`_ that starts at the _`start`_ 'th byte and extends for _`count`_ bytes with _`newsubstring`_. If _`count`_ is omitted, it defaults to the length of _`newsubstring`_.  `overlay('\x1234567890'::bytea placing '\002\003'::bytea from 2 for 3)` → `\x12020390`  
`position` ( _`substring`_ `bytea` `IN` _`bytes`_ `bytea` ) → `integer` Returns first starting index of the specified _`substring`_ within _`bytes`_ , or zero if it's not present.  `position('\x5678'::bytea in '\x1234567890'::bytea)` → `3`  
`rtrim` ( _`bytes`_ `bytea`, _`bytesremoved`_ `bytea` ) → `bytea` Removes the longest string containing only bytes appearing in _`bytesremoved`_ from the end of _`bytes`_.  `rtrim('\x1234567890'::bytea, '\x9012'::bytea)` → `\x12345678`  
`substring` ( _`bytes`_ `bytea` [ `FROM` _`start`_ `integer` ] [ `FOR` _`count`_ `integer` ] ) → `bytea` Extracts the substring of _`bytes`_ starting at the _`start`_ 'th byte if that is specified, and stopping after _`count`_ bytes if that is specified. Provide at least one of _`start`_ and _`count`_.  `substring('\x1234567890'::bytea from 3 for 2)` → `\x5678`  
`trim` ( [ `LEADING` | `TRAILING` | `BOTH` ] _`bytesremoved`_ `bytea` `FROM` _`bytes`_ `bytea` ) → `bytea` Removes the longest string containing only bytes appearing in _`bytesremoved`_ from the start, end, or both ends (`BOTH` is the default) of _`bytes`_.  `trim('\x9012'::bytea from '\x1234567890'::bytea)` → `\x345678`  
`trim` ( [ `LEADING` | `TRAILING` | `BOTH` ] [ `FROM` ] _`bytes`_ `bytea`, _`bytesremoved`_ `bytea` ) → `bytea` This is a non-standard syntax for `trim()`.  `trim(both from '\x1234567890'::bytea, '\x9012'::bytea)` → `\x345678`  
  
  


Additional binary string manipulation functions are available and are listed in [Table 9.12](functions-binarystring.md#FUNCTIONS-BINARYSTRING-OTHER "Table 9.12. Other Binary String Functions"). Some of them are used internally to implement the SQL-standard string functions listed in [Table 9.11](functions-binarystring.md#FUNCTIONS-BINARYSTRING-SQL "Table 9.11. SQL Binary String Functions and Operators"). 

**Table 9.12. Other Binary String Functions**

Function  Description  Example(s)   
---  
`bit_count` ( _`bytes`_ `bytea` ) → `bigint` Returns the number of bits set in the binary string (also known as “popcount”).  `bit_count('\x1234567890'::bytea)` → `15`  
`get_bit` ( _`bytes`_ `bytea`, _`n`_ `bigint` ) → `integer` Extracts [n'th](functions-binarystring.md#FUNCTIONS-ZEROBASED-NOTE) bit from binary string.  `get_bit('\x1234567890'::bytea, 30)` → `1`  
`get_byte` ( _`bytes`_ `bytea`, _`n`_ `integer` ) → `integer` Extracts [n'th](functions-binarystring.md#FUNCTIONS-ZEROBASED-NOTE) byte from binary string.  `get_byte('\x1234567890'::bytea, 4)` → `144`  
`length` ( `bytea` ) → `integer` Returns the number of bytes in the binary string.  `length('\x1234567890'::bytea)` → `5`  
`length` ( _`bytes`_ `bytea`, _`encoding`_ `name` ) → `integer` Returns the number of characters in the binary string, assuming that it is text in the given _`encoding`_.  `length('jose'::bytea, 'UTF8')` → `4`  
`md5` ( `bytea` ) → `text` Computes the MD5 [hash](functions-binarystring.md#FUNCTIONS-HASH-NOTE) of the binary string, with the result written in hexadecimal.  `md5('Th\000omas'::bytea)` → `8ab2d3c9689aaf18​b4958c334c82d8b1`  
`set_bit` ( _`bytes`_ `bytea`, _`n`_ `bigint`, _`newvalue`_ `integer` ) → `bytea` Sets [n'th](functions-binarystring.md#FUNCTIONS-ZEROBASED-NOTE) bit in binary string to _`newvalue`_.  `set_bit('\x1234567890'::bytea, 30, 0)` → `\x1234563890`  
`set_byte` ( _`bytes`_ `bytea`, _`n`_ `integer`, _`newvalue`_ `integer` ) → `bytea` Sets [n'th](functions-binarystring.md#FUNCTIONS-ZEROBASED-NOTE) byte in binary string to _`newvalue`_.  `set_byte('\x1234567890'::bytea, 4, 64)` → `\x1234567840`  
`sha224` ( `bytea` ) → `bytea` Computes the SHA-224 [hash](functions-binarystring.md#FUNCTIONS-HASH-NOTE) of the binary string.  `sha224('abc'::bytea)` → `\x23097d223405d8228642a477bda2​55b32aadbce4bda0b3f7e36c9da7`  
`sha256` ( `bytea` ) → `bytea` Computes the SHA-256 [hash](functions-binarystring.md#FUNCTIONS-HASH-NOTE) of the binary string.  `sha256('abc'::bytea)` → `\xba7816bf8f01cfea414140de5dae2223​b00361a396177a9cb410ff61f20015ad`  
`sha384` ( `bytea` ) → `bytea` Computes the SHA-384 [hash](functions-binarystring.md#FUNCTIONS-HASH-NOTE) of the binary string.  `sha384('abc'::bytea)` → `\xcb00753f45a35e8bb5a03d699ac65007​272c32ab0eded1631a8b605a43ff5bed​8086072ba1e7cc2358baeca134c825a7`  
`sha512` ( `bytea` ) → `bytea` Computes the SHA-512 [hash](functions-binarystring.md#FUNCTIONS-HASH-NOTE) of the binary string.  `sha512('abc'::bytea)` → `\xddaf35a193617abacc417349ae204131​12e6fa4e89a97ea20a9eeee64b55d39a​2192992a274fc1a836ba3c23a3feebbd​454d4423643ce80e2a9ac94fa54ca49f`  
`substr` ( _`bytes`_ `bytea`, _`start`_ `integer` [, _`count`_ `integer` ] ) → `bytea` Extracts the substring of _`bytes`_ starting at the _`start`_ 'th byte, and extending for _`count`_ bytes if that is specified. (Same as `substring(_`bytes`_ from _`start`_ for _`count`_)`.)  `substr('\x1234567890'::bytea, 3, 2)` → `\x5678`  
  
  


Functions `get_byte` and `set_byte` number the first byte of a binary string as byte 0. Functions `get_bit` and `set_bit` number bits from the right within each byte; for example bit 0 is the least significant bit of the first byte, and bit 15 is the most significant bit of the second byte. 

For historical reasons, the function `md5` returns a hex-encoded value of type `text` whereas the SHA-2 functions return type `bytea`. Use the functions [`encode`](functions-binarystring.md#FUNCTION-ENCODE) and [`decode`](functions-binarystring.md#FUNCTION-DECODE) to convert between the two. For example write `encode(sha256('abc'), 'hex')` to get a hex-encoded text representation, or `decode(md5('abc'), 'hex')` to get a `bytea` value. 

Functions for converting strings between different character sets (encodings), and for representing arbitrary binary data in textual form, are shown in [Table 9.13](functions-binarystring.md#FUNCTIONS-BINARYSTRING-CONVERSIONS "Table 9.13. Text/Binary String Conversion Functions"). For these functions, an argument or result of type `text` is expressed in the database's default encoding, while arguments or results of type `bytea` are in an encoding named by another argument. 

**Table 9.13. Text/Binary String Conversion Functions**

Function  Description  Example(s)   
---  
`convert` ( _`bytes`_ `bytea`, _`src_encoding`_ `name`, _`dest_encoding`_ `name` ) → `bytea` Converts a binary string representing text in encoding _`src_encoding`_ to a binary string in encoding _`dest_encoding`_ (see [Section 23.3.4](multibyte.md#MULTIBYTE-CONVERSIONS-SUPPORTED "23.3.4. Available Character Set Conversions") for available conversions).  `convert('text_in_utf8', 'UTF8', 'LATIN1')` → `\x746578745f696e5f75746638`  
`convert_from` ( _`bytes`_ `bytea`, _`src_encoding`_ `name` ) → `text` Converts a binary string representing text in encoding _`src_encoding`_ to `text` in the database encoding (see [Section 23.3.4](multibyte.md#MULTIBYTE-CONVERSIONS-SUPPORTED "23.3.4. Available Character Set Conversions") for available conversions).  `convert_from('text_in_utf8', 'UTF8')` → `text_in_utf8`  
`convert_to` ( _`string`_ `text`, _`dest_encoding`_ `name` ) → `bytea` Converts a `text` string (in the database encoding) to a binary string encoded in encoding _`dest_encoding`_ (see [Section 23.3.4](multibyte.md#MULTIBYTE-CONVERSIONS-SUPPORTED "23.3.4. Available Character Set Conversions") for available conversions).  `convert_to('some_text', 'UTF8')` → `\x736f6d655f74657874`  
`encode` ( _`bytes`_ `bytea`, _`format`_ `text` ) → `text` Encodes binary data into a textual representation; supported _`format`_ values are: [`base64`](functions-binarystring.md#ENCODE-FORMAT-BASE64), [`escape`](functions-binarystring.md#ENCODE-FORMAT-ESCAPE), [`hex`](functions-binarystring.md#ENCODE-FORMAT-HEX).  `encode('123\000\001', 'base64')` → `MTIzAAE=`  
`decode` ( _`string`_ `text`, _`format`_ `text` ) → `bytea` Decodes binary data from a textual representation; supported _`format`_ values are the same as for `encode`.  `decode('MTIzAAE=', 'base64')` → `\x3132330001`  
  
  


The `encode` and `decode` functions support the following textual formats: 

base64  #
    

The `base64` format is that of [RFC 2045 Section 6.8](https://datatracker.ietf.org/doc/html/rfc2045#section-6.8). As per the RFC, encoded lines are broken at 76 characters. However instead of the MIME CRLF end-of-line marker, only a newline is used for end-of-line. The `decode` function ignores carriage-return, newline, space, and tab characters. Otherwise, an error is raised when `decode` is supplied invalid base64 data — including when trailing padding is incorrect. 

escape  #
    

The `escape` format converts zero bytes and bytes with the high bit set into octal escape sequences (`\`_`nnn`_), and it doubles backslashes. Other byte values are represented literally. The `decode` function will raise an error if a backslash is not followed by either a second backslash or three octal digits; it accepts other byte values unchanged. 

hex  #
    

The `hex` format represents each 4 bits of data as one hexadecimal digit, `0` through `f`, writing the higher-order digit of each byte first. The `encode` function outputs the `a`-`f` hex digits in lower case. Because the smallest unit of data is 8 bits, there are always an even number of characters returned by `encode`. The `decode` function accepts the `a`-`f` characters in either upper or lower case. An error is raised when `decode` is given invalid hex data — including when given an odd number of characters. 

See also the aggregate function `string_agg` in [Section 9.21](functions-aggregate.md "9.21. Aggregate Functions") and the large object functions in [Section 33.4](lo-funcs.md "33.4. Server-Side Functions"). 

* * *

[Prev](functions-string.md "9.4. String Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")|  [Next](functions-bitstring.md "9.6. Bit String Functions and Operators")  
---|---|---  
9.4. String Functions and Operators | [Home](index.md "PostgreSQL 17.5 Documentation")|  9.6. Bit String Functions and Operators
