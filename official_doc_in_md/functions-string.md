9.4. String Functions and Operators  
---  
[Prev](functions-math.md "9.3. Mathematical Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")| Chapter 9. Functions and Operators| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](functions-binarystring.md "9.5. Binary String Functions and Operators")  
  
* * *

## 9.4. String Functions and Operators #

[9.4.1. `format`](functions-string.md#FUNCTIONS-STRING-FORMAT)

This section describes functions and operators for examining and manipulating string values. Strings in this context include values of the types `character`, `character varying`, and `text`. Except where noted, these functions and operators are declared to accept and return type `text`. They will interchangeably accept `character varying` arguments. Values of type `character` will be converted to `text` before the function or operator is applied, resulting in stripping any trailing spaces in the `character` value. 

SQL defines some string functions that use key words, rather than commas, to separate arguments. Details are in [Table 9.9](functions-string.md#FUNCTIONS-STRING-SQL "Table 9.9. SQL String Functions and Operators"). PostgreSQL also provides versions of these functions that use the regular function invocation syntax (see [Table 9.10](functions-string.md#FUNCTIONS-STRING-OTHER "Table 9.10. Other String Functions and Operators")). 

### Note

The string concatenation operator (`||`) will accept non-string input, so long as at least one input is of string type, as shown in [Table 9.9](functions-string.md#FUNCTIONS-STRING-SQL "Table 9.9. SQL String Functions and Operators"). For other cases, inserting an explicit coercion to `text` can be used to have non-string input accepted. 

**Table 9.9. SQL String Functions and Operators**

Function/Operator  Description  Example(s)   
---  
`text` `||` `text` → `text` Concatenates the two strings.  `'Post' || 'greSQL'` → `PostgreSQL`  
`text` `||` `anynonarray` → `text` `anynonarray` `||` `text` → `text` Converts the non-string input to text, then concatenates the two strings. (The non-string input cannot be of an array type, because that would create ambiguity with the array `||` operators. If you want to concatenate an array's text equivalent, cast it to `text` explicitly.)  `'Value: ' || 42` → `Value: 42`  
`btrim` ( _`string`_ `text` [, _`characters`_ `text` ] ) → `text` Removes the longest string containing only characters in _`characters`_ (a space by default) from the start and end of _`string`_.  `btrim('xyxtrimyyx', 'xyz')` → `trim`  
`text` `IS` [`NOT`] [_`form`_] `NORMALIZED` → `boolean` Checks whether the string is in the specified Unicode normalization form. The optional _`form`_ key word specifies the form: `NFC` (the default), `NFD`, `NFKC`, or `NFKD`. This expression can only be used when the server encoding is `UTF8`. Note that checking for normalization using this expression is often faster than normalizing possibly already normalized strings.  `U&'\0061\0308bc' IS NFD NORMALIZED` → `t`  
`bit_length` ( `text` ) → `integer` Returns number of bits in the string (8 times the `octet_length`).  `bit_length('jose')` → `32`  
`char_length` ( `text` ) → `integer` `character_length` ( `text` ) → `integer` Returns number of characters in the string.  `char_length('josé')` → `4`  
`lower` ( `text` ) → `text` Converts the string to all lower case, according to the rules of the database's locale.  `lower('TOM')` → `tom`  
`lpad` ( _`string`_ `text`, _`length`_ `integer` [, _`fill`_ `text` ] ) → `text` Extends the _`string`_ to length _`length`_ by prepending the characters _`fill`_ (a space by default). If the _`string`_ is already longer than _`length`_ then it is truncated (on the right).  `lpad('hi', 5, 'xy')` → `xyxhi`  
`ltrim` ( _`string`_ `text` [, _`characters`_ `text` ] ) → `text` Removes the longest string containing only characters in _`characters`_ (a space by default) from the start of _`string`_.  `ltrim('zzzytest', 'xyz')` → `test`  
`normalize` ( `text` [, _`form`_ ] ) → `text` Converts the string to the specified Unicode normalization form. The optional _`form`_ key word specifies the form: `NFC` (the default), `NFD`, `NFKC`, or `NFKD`. This function can only be used when the server encoding is `UTF8`.  `normalize(U&'\0061\0308bc', NFC)` → `U&'\00E4bc'`  
`octet_length` ( `text` ) → `integer` Returns number of bytes in the string.  `octet_length('josé')` → `5` (if server encoding is UTF8)   
`octet_length` ( `character` ) → `integer` Returns number of bytes in the string. Since this version of the function accepts type `character` directly, it will not strip trailing spaces.  `octet_length('abc '::character(4))` → `4`  
`overlay` ( _`string`_ `text` `PLACING` _`newsubstring`_ `text` `FROM` _`start`_ `integer` [ `FOR` _`count`_ `integer` ] ) → `text` Replaces the substring of _`string`_ that starts at the _`start`_ 'th character and extends for _`count`_ characters with _`newsubstring`_. If _`count`_ is omitted, it defaults to the length of _`newsubstring`_.  `overlay('Txxxxas' placing 'hom' from 2 for 4)` → `Thomas`  
`position` ( _`substring`_ `text` `IN` _`string`_ `text` ) → `integer` Returns first starting index of the specified _`substring`_ within _`string`_ , or zero if it's not present.  `position('om' in 'Thomas')` → `3`  
`rpad` ( _`string`_ `text`, _`length`_ `integer` [, _`fill`_ `text` ] ) → `text` Extends the _`string`_ to length _`length`_ by appending the characters _`fill`_ (a space by default). If the _`string`_ is already longer than _`length`_ then it is truncated.  `rpad('hi', 5, 'xy')` → `hixyx`  
`rtrim` ( _`string`_ `text` [, _`characters`_ `text` ] ) → `text` Removes the longest string containing only characters in _`characters`_ (a space by default) from the end of _`string`_.  `rtrim('testxxzx', 'xyz')` → `test`  
`substring` ( _`string`_ `text` [ `FROM` _`start`_ `integer` ] [ `FOR` _`count`_ `integer` ] ) → `text` Extracts the substring of _`string`_ starting at the _`start`_ 'th character if that is specified, and stopping after _`count`_ characters if that is specified. Provide at least one of _`start`_ and _`count`_.  `substring('Thomas' from 2 for 3)` → `hom` `substring('Thomas' from 3)` → `omas` `substring('Thomas' for 2)` → `Th`  
`substring` ( _`string`_ `text` `FROM` _`pattern`_ `text` ) → `text` Extracts the first substring matching POSIX regular expression; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `substring('Thomas' from '...$')` → `mas`  
`substring` ( _`string`_ `text` `SIMILAR` _`pattern`_ `text` `ESCAPE` _`escape`_ `text` ) → `text` `substring` ( _`string`_ `text` `FROM` _`pattern`_ `text` `FOR` _`escape`_ `text` ) → `text` Extracts the first substring matching SQL regular expression; see [Section 9.7.2](functions-matching.md#FUNCTIONS-SIMILARTO-REGEXP "9.7.2. SIMILAR TO Regular Expressions"). The first form has been specified since SQL:2003; the second form was only in SQL:1999 and should be considered obsolete.  `substring('Thomas' similar '%#"o_a#"_' escape '#')` → `oma`  
`trim` ( [ `LEADING` | `TRAILING` | `BOTH` ] [ _`characters`_ `text` ] `FROM` _`string`_ `text` ) → `text` Removes the longest string containing only characters in _`characters`_ (a space by default) from the start, end, or both ends (`BOTH` is the default) of _`string`_.  `trim(both 'xyz' from 'yxTomxx')` → `Tom`  
`trim` ( [ `LEADING` | `TRAILING` | `BOTH` ] [ `FROM` ] _`string`_ `text` [, _`characters`_ `text` ] ) → `text` This is a non-standard syntax for `trim()`.  `trim(both from 'yxTomxx', 'xyz')` → `Tom`  
`unicode_assigned` ( `text` ) → `boolean` Returns `true` if all characters in the string are assigned Unicode codepoints; `false` otherwise. This function can only be used when the server encoding is `UTF8`.   
`upper` ( `text` ) → `text` Converts the string to all upper case, according to the rules of the database's locale.  `upper('tom')` → `TOM`  
  
  


Additional string manipulation functions and operators are available and are listed in [Table 9.10](functions-string.md#FUNCTIONS-STRING-OTHER "Table 9.10. Other String Functions and Operators"). (Some of these are used internally to implement the SQL-standard string functions listed in [Table 9.9](functions-string.md#FUNCTIONS-STRING-SQL "Table 9.9. SQL String Functions and Operators").) There are also pattern-matching operators, which are described in [Section 9.7](functions-matching.md "9.7. Pattern Matching"), and operators for full-text search, which are described in [Chapter 12](textsearch.md "Chapter 12. Full Text Search"). 

**Table 9.10. Other String Functions and Operators**

Function/Operator  Description  Example(s)   
---  
`text` `^@` `text` → `boolean` Returns true if the first string starts with the second string (equivalent to the `starts_with()` function).  `'alphabet' ^@ 'alph'` → `t`  
`ascii` ( `text` ) → `integer` Returns the numeric code of the first character of the argument. In UTF8 encoding, returns the Unicode code point of the character. In other multibyte encodings, the argument must be an ASCII character.  `ascii('x')` → `120`  
`chr` ( `integer` ) → `text` Returns the character with the given code. In UTF8 encoding the argument is treated as a Unicode code point. In other multibyte encodings the argument must designate an ASCII character. `chr(0)` is disallowed because text data types cannot store that character.  `chr(65)` → `A`  
`concat` ( _`val1`_ `"any"` [, _`val2`_ `"any"` [, ...] ] ) → `text` Concatenates the text representations of all the arguments. NULL arguments are ignored.  `concat('abcde', 2, NULL, 22)` → `abcde222`  
`concat_ws` ( _`sep`_ `text`, _`val1`_ `"any"` [, _`val2`_ `"any"` [, ...] ] ) → `text` Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored.  `concat_ws(',', 'abcde', 2, NULL, 22)` → `abcde,2,22`  
`format` ( _`formatstr`_ `text` [, _`formatarg`_ `"any"` [, ...] ] ) → `text` Formats arguments according to a format string; see [Section 9.4.1](functions-string.md#FUNCTIONS-STRING-FORMAT "9.4.1. format"). This function is similar to the C function `sprintf`.  `format('Hello %s, %1$s', 'World')` → `Hello World, World`  
`initcap` ( `text` ) → `text` Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.  `initcap('hi THOMAS')` → `Hi Thomas`  
`left` ( _`string`_ `text`, _`n`_ `integer` ) → `text` Returns first _`n`_ characters in the string, or when _`n`_ is negative, returns all but last |_`n`_ | characters.  `left('abcde', 2)` → `ab`  
`length` ( `text` ) → `integer` Returns the number of characters in the string.  `length('jose')` → `4`  
`md5` ( `text` ) → `text` Computes the MD5 [hash](functions-binarystring.md#FUNCTIONS-HASH-NOTE) of the argument, with the result written in hexadecimal.  `md5('abc')` → `900150983cd24fb0​d6963f7d28e17f72`  
`parse_ident` ( _`qualified_identifier`_ `text` [, _`strict_mode`_ `boolean` `DEFAULT` `true` ] ) → `text[]` Splits _`qualified_identifier`_ into an array of identifiers, removing any quoting of individual identifiers. By default, extra characters after the last identifier are considered an error; but if the second parameter is `false`, then such extra characters are ignored. (This behavior is useful for parsing names for objects like functions.) Note that this function does not truncate over-length identifiers. If you want truncation you can cast the result to `name[]`.  `parse_ident('"SomeSchema".someTable')` → `{SomeSchema,sometable}`  
`pg_client_encoding` ( ) → `name` Returns current client encoding name.  `pg_client_encoding()` → `UTF8`  
`quote_ident` ( `text` ) → `text` Returns the given string suitably quoted to be used as an identifier in an SQL statement string. Quotes are added only if necessary (i.e., if the string contains non-identifier characters or would be case-folded). Embedded quotes are properly doubled. See also [Example 41.1](plpgsql-statements.md#PLPGSQL-QUOTE-LITERAL-EXAMPLE "Example 41.1. Quoting Values in Dynamic Queries").  `quote_ident('Foo bar')` → `"Foo bar"`  
`quote_literal` ( `text` ) → `text` Returns the given string suitably quoted to be used as a string literal in an SQL statement string. Embedded single-quotes and backslashes are properly doubled. Note that `quote_literal` returns null on null input; if the argument might be null, `quote_nullable` is often more suitable. See also [Example 41.1](plpgsql-statements.md#PLPGSQL-QUOTE-LITERAL-EXAMPLE "Example 41.1. Quoting Values in Dynamic Queries").  `quote_literal(E'O\'Reilly')` → `'O''Reilly'`  
`quote_literal` ( `anyelement` ) → `text` Converts the given value to text and then quotes it as a literal. Embedded single-quotes and backslashes are properly doubled.  `quote_literal(42.5)` → `'42.5'`  
`quote_nullable` ( `text` ) → `text` Returns the given string suitably quoted to be used as a string literal in an SQL statement string; or, if the argument is null, returns `NULL`. Embedded single-quotes and backslashes are properly doubled. See also [Example 41.1](plpgsql-statements.md#PLPGSQL-QUOTE-LITERAL-EXAMPLE "Example 41.1. Quoting Values in Dynamic Queries").  `quote_nullable(NULL)` → `NULL`  
`quote_nullable` ( `anyelement` ) → `text` Converts the given value to text and then quotes it as a literal; or, if the argument is null, returns `NULL`. Embedded single-quotes and backslashes are properly doubled.  `quote_nullable(42.5)` → `'42.5'`  
`regexp_count` ( _`string`_ `text`, _`pattern`_ `text` [, _`start`_ `integer` [, _`flags`_ `text` ] ] ) → `integer` Returns the number of times the POSIX regular expression _`pattern`_ matches in the _`string`_ ; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `regexp_count('123456789012', '\d\d\d', 2)` → `3`  
`regexp_instr` ( _`string`_ `text`, _`pattern`_ `text` [, _`start`_ `integer` [, _`N`_ `integer` [, _`endoption`_ `integer` [, _`flags`_ `text` [, _`subexpr`_ `integer` ] ] ] ] ] ) → `integer` Returns the position within _`string`_ where the _`N`_ 'th match of the POSIX regular expression _`pattern`_ occurs, or zero if there is no such match; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `regexp_instr('ABCDEF', 'c(.)(..)', 1, 1, 0, 'i')` → `3` `regexp_instr('ABCDEF', 'c(.)(..)', 1, 1, 0, 'i', 2)` → `5`  
`regexp_like` ( _`string`_ `text`, _`pattern`_ `text` [, _`flags`_ `text` ] ) → `boolean` Checks whether a match of the POSIX regular expression _`pattern`_ occurs within _`string`_ ; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `regexp_like('Hello World', 'world$', 'i')` → `t`  
`regexp_match` ( _`string`_ `text`, _`pattern`_ `text` [, _`flags`_ `text` ] ) → `text[]` Returns substrings within the first match of the POSIX regular expression _`pattern`_ to the _`string`_ ; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `regexp_match('foobarbequebaz', '(bar)(beque)')` → `{bar,beque}`  
`regexp_matches` ( _`string`_ `text`, _`pattern`_ `text` [, _`flags`_ `text` ] ) → `setof text[]` Returns substrings within the first match of the POSIX regular expression _`pattern`_ to the _`string`_ , or substrings within all such matches if the `g` flag is used; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `regexp_matches('foobarbequebaz', 'ba.', 'g')` → ``
    
    
     {bar}
     {baz}
      
  
`regexp_replace` ( _`string`_ `text`, _`pattern`_ `text`, _`replacement`_ `text` [, _`start`_ `integer` ] [, _`flags`_ `text` ] ) → `text` Replaces the substring that is the first match to the POSIX regular expression _`pattern`_ , or all such matches if the `g` flag is used; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `regexp_replace('Thomas', '.[mN]a.', 'M')` → `ThM`  
`regexp_replace` ( _`string`_ `text`, _`pattern`_ `text`, _`replacement`_ `text`, _`start`_ `integer`, _`N`_ `integer` [, _`flags`_ `text` ] ) → `text` Replaces the substring that is the _`N`_ 'th match to the POSIX regular expression _`pattern`_ , or all such matches if _`N`_ is zero; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `regexp_replace('Thomas', '.', 'X', 3, 2)` → `ThoXas`  
`regexp_split_to_array` ( _`string`_ `text`, _`pattern`_ `text` [, _`flags`_ `text` ] ) → `text[]` Splits _`string`_ using a POSIX regular expression as the delimiter, producing an array of results; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `regexp_split_to_array('hello world', '\s+')` → `{hello,world}`  
`regexp_split_to_table` ( _`string`_ `text`, _`pattern`_ `text` [, _`flags`_ `text` ] ) → `setof text` Splits _`string`_ using a POSIX regular expression as the delimiter, producing a set of results; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `regexp_split_to_table('hello world', '\s+')` → ``
    
    
     hello
     world
      
  
`regexp_substr` ( _`string`_ `text`, _`pattern`_ `text` [, _`start`_ `integer` [, _`N`_ `integer` [, _`flags`_ `text` [, _`subexpr`_ `integer` ] ] ] ] ) → `text` Returns the substring within _`string`_ that matches the _`N`_ 'th occurrence of the POSIX regular expression _`pattern`_ , or `NULL` if there is no such match; see [Section 9.7.3](functions-matching.md#FUNCTIONS-POSIX-REGEXP "9.7.3. POSIX Regular Expressions").  `regexp_substr('ABCDEF', 'c(.)(..)', 1, 1, 'i')` → `CDEF` `regexp_substr('ABCDEF', 'c(.)(..)', 1, 1, 'i', 2)` → `EF`  
`repeat` ( _`string`_ `text`, _`number`_ `integer` ) → `text` Repeats _`string`_ the specified _`number`_ of times.  `repeat('Pg', 4)` → `PgPgPgPg`  
`replace` ( _`string`_ `text`, _`from`_ `text`, _`to`_ `text` ) → `text` Replaces all occurrences in _`string`_ of substring _`from`_ with substring _`to`_.  `replace('abcdefabcdef', 'cd', 'XX')` → `abXXefabXXef`  
`reverse` ( `text` ) → `text` Reverses the order of the characters in the string.  `reverse('abcde')` → `edcba`  
`right` ( _`string`_ `text`, _`n`_ `integer` ) → `text` Returns last _`n`_ characters in the string, or when _`n`_ is negative, returns all but first |_`n`_ | characters.  `right('abcde', 2)` → `de`  
`split_part` ( _`string`_ `text`, _`delimiter`_ `text`, _`n`_ `integer` ) → `text` Splits _`string`_ at occurrences of _`delimiter`_ and returns the _`n`_ 'th field (counting from one), or when _`n`_ is negative, returns the |_`n`_ |'th-from-last field.  `split_part('abc~@~def~@~ghi', '~@~', 2)` → `def` `split_part('abc,def,ghi,jkl', ',', -2)` → `ghi`  
`starts_with` ( _`string`_ `text`, _`prefix`_ `text` ) → `boolean` Returns true if _`string`_ starts with _`prefix`_.  `starts_with('alphabet', 'alph')` → `t`  
`string_to_array` ( _`string`_ `text`, _`delimiter`_ `text` [, _`null_string`_ `text` ] ) → `text[]` Splits the _`string`_ at occurrences of _`delimiter`_ and forms the resulting fields into a `text` array. If _`delimiter`_ is `NULL`, each character in the _`string`_ will become a separate element in the array. If _`delimiter`_ is an empty string, then the _`string`_ is treated as a single field. If _`null_string`_ is supplied and is not `NULL`, fields matching that string are replaced by `NULL`. See also [`array_to_string`](functions-array.md#FUNCTION-ARRAY-TO-STRING).  `string_to_array('xx~~yy~~zz', '~~', 'yy')` → `{xx,NULL,zz}`  
`string_to_table` ( _`string`_ `text`, _`delimiter`_ `text` [, _`null_string`_ `text` ] ) → `setof text` Splits the _`string`_ at occurrences of _`delimiter`_ and returns the resulting fields as a set of `text` rows. If _`delimiter`_ is `NULL`, each character in the _`string`_ will become a separate row of the result. If _`delimiter`_ is an empty string, then the _`string`_ is treated as a single field. If _`null_string`_ is supplied and is not `NULL`, fields matching that string are replaced by `NULL`.  `string_to_table('xx~^~yy~^~zz', '~^~', 'yy')` → ``
    
    
     xx
     NULL
     zz
      
  
`strpos` ( _`string`_ `text`, _`substring`_ `text` ) → `integer` Returns first starting index of the specified _`substring`_ within _`string`_ , or zero if it's not present. (Same as `position(_`substring`_ in _`string`_)`, but note the reversed argument order.)  `strpos('high', 'ig')` → `2`  
`substr` ( _`string`_ `text`, _`start`_ `integer` [, _`count`_ `integer` ] ) → `text` Extracts the substring of _`string`_ starting at the _`start`_ 'th character, and extending for _`count`_ characters if that is specified. (Same as `substring(_`string`_ from _`start`_ for _`count`_)`.)  `substr('alphabet', 3)` → `phabet` `substr('alphabet', 3, 2)` → `ph`  
`to_ascii` ( _`string`_ `text` ) → `text` `to_ascii` ( _`string`_ `text`, _`encoding`_ `name` ) → `text` `to_ascii` ( _`string`_ `text`, _`encoding`_ `integer` ) → `text` Converts _`string`_ to ASCII from another encoding, which may be identified by name or number. If _`encoding`_ is omitted the database encoding is assumed (which in practice is the only useful case). The conversion consists primarily of dropping accents. Conversion is only supported from `LATIN1`, `LATIN2`, `LATIN9`, and `WIN1250` encodings. (See the [unaccent](unaccent.md "F.46. unaccent — a text search dictionary which removes diacritics") module for another, more flexible solution.)  `to_ascii('Karél')` → `Karel`  
`to_bin` ( `integer` ) → `text` `to_bin` ( `bigint` ) → `text` Converts the number to its equivalent two's complement binary representation.  `to_bin(2147483647)` → `1111111111111111111111111111111` `to_bin(-1234)` → `11111111111111111111101100101110`  
`to_hex` ( `integer` ) → `text` `to_hex` ( `bigint` ) → `text` Converts the number to its equivalent two's complement hexadecimal representation.  `to_hex(2147483647)` → `7fffffff` `to_hex(-1234)` → `fffffb2e`  
`to_oct` ( `integer` ) → `text` `to_oct` ( `bigint` ) → `text` Converts the number to its equivalent two's complement octal representation.  `to_oct(2147483647)` → `17777777777` `to_oct(-1234)` → `37777775456`  
`translate` ( _`string`_ `text`, _`from`_ `text`, _`to`_ `text` ) → `text` Replaces each character in _`string`_ that matches a character in the _`from`_ set with the corresponding character in the _`to`_ set. If _`from`_ is longer than _`to`_ , occurrences of the extra characters in _`from`_ are deleted.  `translate('12345', '143', 'ax')` → `a2x5`  
`unistr` ( `text` ) → `text` Evaluate escaped Unicode characters in the argument. Unicode characters can be specified as `\_`XXXX`_` (4 hexadecimal digits), `\+_`XXXXXX`_` (6 hexadecimal digits), `\u _`XXXX`_` (4 hexadecimal digits), or `\U _`XXXXXXXX`_` (8 hexadecimal digits). To specify a backslash, write two backslashes. All other characters are taken literally.  If the server encoding is not UTF-8, the Unicode code point identified by one of these escape sequences is converted to the actual server encoding; an error is reported if that's not possible.  This function provides a (non-standard) alternative to string constants with Unicode escapes (see [Section 4.1.2.3](sql-syntax-lexical.md#SQL-SYNTAX-STRINGS-UESCAPE "4.1.2.3. String Constants with Unicode Escapes")).  `unistr('d\0061t\+000061')` → `data` `unistr('d\u0061t\U00000061')` → `data`  
  
  


The `concat`, `concat_ws` and `format` functions are variadic, so it is possible to pass the values to be concatenated or formatted as an array marked with the `VARIADIC` keyword (see [Section 36.5.6](xfunc-sql.md#XFUNC-SQL-VARIADIC-FUNCTIONS "36.5.6. SQL Functions with Variable Numbers of Arguments")). The array's elements are treated as if they were separate ordinary arguments to the function. If the variadic array argument is NULL, `concat` and `concat_ws` return NULL, but `format` treats a NULL as a zero-element array. 

See also the aggregate function `string_agg` in [Section 9.21](functions-aggregate.md "9.21. Aggregate Functions"), and the functions for converting between strings and the `bytea` type in [Table 9.13](functions-binarystring.md#FUNCTIONS-BINARYSTRING-CONVERSIONS "Table 9.13. Text/Binary String Conversion Functions"). 

### 9.4.1. `format` #

The function `format` produces output formatted according to a format string, in a style similar to the C function `sprintf`. 
    
    
    format(_formatstr_ text [, _formatarg_ "any" [, ...] ])
    

_`formatstr`_ is a format string that specifies how the result should be formatted. Text in the format string is copied directly to the result, except where _format specifiers_ are used. Format specifiers act as placeholders in the string, defining how subsequent function arguments should be formatted and inserted into the result. Each _`formatarg`_ argument is converted to text according to the usual output rules for its data type, and then formatted and inserted into the result string according to the format specifier(s). 

Format specifiers are introduced by a `%` character and have the form 
    
    
    %[_position_][_flags_][_width_]_type_
    

where the component fields are: 

_`position`_ (optional)
    

A string of the form `_`n`_ $` where _`n`_ is the index of the argument to print. Index 1 means the first argument after _`formatstr`_. If the _`position`_ is omitted, the default is to use the next argument in sequence. 

_`flags`_ (optional)
    

Additional options controlling how the format specifier's output is formatted. Currently the only supported flag is a minus sign (`-`) which will cause the format specifier's output to be left-justified. This has no effect unless the _`width`_ field is also specified. 

_`width`_ (optional)
    

Specifies the _minimum_ number of characters to use to display the format specifier's output. The output is padded on the left or right (depending on the `-` flag) with spaces as needed to fill the width. A too-small width does not cause truncation of the output, but is simply ignored. The width may be specified using any of the following: a positive integer; an asterisk (`*`) to use the next function argument as the width; or a string of the form `*_`n`_ $` to use the _`n`_ th function argument as the width. 

If the width comes from a function argument, that argument is consumed before the argument that is used for the format specifier's value. If the width argument is negative, the result is left aligned (as if the `-` flag had been specified) within a field of length `abs`(_`width`_). 

_`type`_ (required)
    

The type of format conversion to use to produce the format specifier's output. The following types are supported: 

  * `s` formats the argument value as a simple string. A null value is treated as an empty string. 

  * `I` treats the argument value as an SQL identifier, double-quoting it if necessary. It is an error for the value to be null (equivalent to `quote_ident`). 

  * `L` quotes the argument value as an SQL literal. A null value is displayed as the string `NULL`, without quotes (equivalent to `quote_nullable`). 




In addition to the format specifiers described above, the special sequence `%%` may be used to output a literal `%` character. 

Here are some examples of the basic format conversions: 
    
    
    SELECT format('Hello %s', 'World');
    _Result:_Hello World
    
    SELECT format('Testing %s, %s, %s, %%', 'one', 'two', 'three');
    _Result:_Testing one, two, three, %
    
    SELECT format('INSERT INTO %I VALUES(%L)', 'Foo bar', E'O\'Reilly');
    _Result:_INSERT INTO "Foo bar" VALUES('O''Reilly')
    
    SELECT format('INSERT INTO %I VALUES(%L)', 'locations', 'C:\Program Files');
    _Result:_INSERT INTO locations VALUES('C:\Program Files')
    

Here are examples using _`width`_ fields and the `-` flag: 
    
    
    SELECT format('|%10s|', 'foo');
    _Result:_|       foo|
    
    SELECT format('|%-10s|', 'foo');
    _Result:_|foo       |
    
    SELECT format('|%*s|', 10, 'foo');
    _Result:_|       foo|
    
    SELECT format('|%*s|', -10, 'foo');
    _Result:_|foo       |
    
    SELECT format('|%-*s|', 10, 'foo');
    _Result:_|foo       |
    
    SELECT format('|%-*s|', -10, 'foo');
    _Result:_|foo       |
    

These examples show use of _`position`_ fields: 
    
    
    SELECT format('Testing %3$s, %2$s, %1$s', 'one', 'two', 'three');
    _Result:_Testing three, two, one
    
    SELECT format('|%*2$s|', 'foo', 10, 'bar');
    _Result:_|       bar|
    
    SELECT format('|%1$*2$s|', 'foo', 10, 'bar');
    _Result:_|       foo|
    

Unlike the standard C function `sprintf`, PostgreSQL's `format` function allows format specifiers with and without _`position`_ fields to be mixed in the same format string. A format specifier without a _`position`_ field always uses the next argument after the last argument consumed. In addition, the `format` function does not require all function arguments to be used in the format string. For example: 
    
    
    SELECT format('Testing %3$s, %2$s, %s', 'one', 'two', 'three');
    _Result:_Testing three, two, three
    

The `%I` and `%L` format specifiers are particularly useful for safely constructing dynamic SQL statements. See [Example 41.1](plpgsql-statements.md#PLPGSQL-QUOTE-LITERAL-EXAMPLE "Example 41.1. Quoting Values in Dynamic Queries"). 

* * *

[Prev](functions-math.md "9.3. Mathematical Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")|  [Next](functions-binarystring.md "9.5. Binary String Functions and Operators")  
---|---|---  
9.3. Mathematical Functions and Operators | [Home](index.md "PostgreSQL 17.5 Documentation")|  9.5. Binary String Functions and Operators
