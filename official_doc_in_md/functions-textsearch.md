9.13. Text Search Functions and Operators  
---  
[Prev](functions-net.md "9.12. Network Address Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")| Chapter 9. Functions and Operators| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](functions-uuid.md "9.14. UUID Functions")  
  
* * *

## 9.13. Text Search Functions and Operators #

[Table 9.42](functions-textsearch.md#TEXTSEARCH-OPERATORS-TABLE "Table 9.42. Text Search Operators"), [Table 9.43](functions-textsearch.md#TEXTSEARCH-FUNCTIONS-TABLE "Table 9.43. Text Search Functions") and [Table 9.44](functions-textsearch.md#TEXTSEARCH-FUNCTIONS-DEBUG-TABLE "Table 9.44. Text Search Debugging Functions") summarize the functions and operators that are provided for full text searching. See [Chapter 12](textsearch.md "Chapter 12. Full Text Search") for a detailed explanation of PostgreSQL's text search facility. 

**Table 9.42. Text Search Operators**

Operator  Description  Example(s)   
---  
`tsvector` `@@` `tsquery` → `boolean` `tsquery` `@@` `tsvector` → `boolean` Does `tsvector` match `tsquery`? (The arguments can be given in either order.)  `to_tsvector('fat cats ate rats') @@ to_tsquery('cat & rat')` → `t`  
`text` `@@` `tsquery` → `boolean` Does text string, after implicit invocation of `to_tsvector()`, match `tsquery`?  `'fat cats ate rats' @@ to_tsquery('cat & rat')` → `t`  
`tsvector` `||` `tsvector` → `tsvector` Concatenates two `tsvector`s. If both inputs contain lexeme positions, the second input's positions are adjusted accordingly.  `'a:1 b:2'::tsvector || 'c:1 d:2 b:3'::tsvector` → `'a':1 'b':2,5 'c':3 'd':4`  
`tsquery` `&&` `tsquery` → `tsquery` ANDs two `tsquery`s together, producing a query that matches documents that match both input queries.  `'fat | rat'::tsquery && 'cat'::tsquery` → `( 'fat' | 'rat' ) & 'cat'`  
`tsquery` `||` `tsquery` → `tsquery` ORs two `tsquery`s together, producing a query that matches documents that match either input query.  `'fat | rat'::tsquery || 'cat'::tsquery` → `'fat' | 'rat' | 'cat'`  
`!!` `tsquery` → `tsquery` Negates a `tsquery`, producing a query that matches documents that do not match the input query.  `!! 'cat'::tsquery` → `!'cat'`  
`tsquery` `<->` `tsquery` → `tsquery` Constructs a phrase query, which matches if the two input queries match at successive lexemes.  `to_tsquery('fat') <-> to_tsquery('rat')` → `'fat' <-> 'rat'`  
`tsquery` `@>` `tsquery` → `boolean` Does first `tsquery` contain the second? (This considers only whether all the lexemes appearing in one query appear in the other, ignoring the combining operators.)  `'cat'::tsquery @> 'cat & rat'::tsquery` → `f`  
`tsquery` `<@` `tsquery` → `boolean` Is first `tsquery` contained in the second? (This considers only whether all the lexemes appearing in one query appear in the other, ignoring the combining operators.)  `'cat'::tsquery <@ 'cat & rat'::tsquery` → `t` `'cat'::tsquery <@ '!cat & rat'::tsquery` → `t`  
  
  


In addition to these specialized operators, the usual comparison operators shown in [Table 9.1](functions-comparison.md#FUNCTIONS-COMPARISON-OP-TABLE "Table 9.1. Comparison Operators") are available for types `tsvector` and `tsquery`. These are not very useful for text searching but allow, for example, unique indexes to be built on columns of these types. 

**Table 9.43. Text Search Functions**

Function  Description  Example(s)   
---  
`array_to_tsvector` ( `text[]` ) → `tsvector` Converts an array of text strings to a `tsvector`. The given strings are used as lexemes as-is, without further processing. Array elements must not be empty strings or `NULL`.  `array_to_tsvector('{fat,cat,rat}'::text[])` → `'cat' 'fat' 'rat'`  
`get_current_ts_config` ( ) → `regconfig` Returns the OID of the current default text search configuration (as set by [default_text_search_config](runtime-config-client.md#GUC-DEFAULT-TEXT-SEARCH-CONFIG)).  `get_current_ts_config()` → `english`  
`length` ( `tsvector` ) → `integer` Returns the number of lexemes in the `tsvector`.  `length('fat:2,4 cat:3 rat:5A'::tsvector)` → `3`  
`numnode` ( `tsquery` ) → `integer` Returns the number of lexemes plus operators in the `tsquery`.  `numnode('(fat & rat) | cat'::tsquery)` → `5`  
`plainto_tsquery` ( [ _`config`_ `regconfig`, ] _`query`_ `text` ) → `tsquery` Converts text to a `tsquery`, normalizing words according to the specified or default configuration. Any punctuation in the string is ignored (it does not determine query operators). The resulting query matches documents containing all non-stopwords in the text.  `plainto_tsquery('english', 'The Fat Rats')` → `'fat' & 'rat'`  
`phraseto_tsquery` ( [ _`config`_ `regconfig`, ] _`query`_ `text` ) → `tsquery` Converts text to a `tsquery`, normalizing words according to the specified or default configuration. Any punctuation in the string is ignored (it does not determine query operators). The resulting query matches phrases containing all non-stopwords in the text.  `phraseto_tsquery('english', 'The Fat Rats')` → `'fat' <-> 'rat'` `phraseto_tsquery('english', 'The Cat and Rats')` → `'cat' <2> 'rat'`  
`websearch_to_tsquery` ( [ _`config`_ `regconfig`, ] _`query`_ `text` ) → `tsquery` Converts text to a `tsquery`, normalizing words according to the specified or default configuration. Quoted word sequences are converted to phrase tests. The word “or” is understood as producing an OR operator, and a dash produces a NOT operator; other punctuation is ignored. This approximates the behavior of some common web search tools.  `websearch_to_tsquery('english', '"fat rat" or cat dog')` → `'fat' <-> 'rat' | 'cat' & 'dog'`  
`querytree` ( `tsquery` ) → `text` Produces a representation of the indexable portion of a `tsquery`. A result that is empty or just `T` indicates a non-indexable query.  `querytree('foo & ! bar'::tsquery)` → `'foo'`  
`setweight` ( _`vector`_ `tsvector`, _`weight`_ `"char"` ) → `tsvector` Assigns the specified _`weight`_ to each element of the _`vector`_.  `setweight('fat:2,4 cat:3 rat:5B'::tsvector, 'A')` → `'cat':3A 'fat':2A,4A 'rat':5A`  
`setweight` ( _`vector`_ `tsvector`, _`weight`_ `"char"`, _`lexemes`_ `text[]` ) → `tsvector` Assigns the specified _`weight`_ to elements of the _`vector`_ that are listed in _`lexemes`_. The strings in _`lexemes`_ are taken as lexemes as-is, without further processing. Strings that do not match any lexeme in _`vector`_ are ignored.  `setweight('fat:2,4 cat:3 rat:5,6B'::tsvector, 'A', '{cat,rat}')` → `'cat':3A 'fat':2,4 'rat':5A,6A`  
`strip` ( `tsvector` ) → `tsvector` Removes positions and weights from the `tsvector`.  `strip('fat:2,4 cat:3 rat:5A'::tsvector)` → `'cat' 'fat' 'rat'`  
`to_tsquery` ( [ _`config`_ `regconfig`, ] _`query`_ `text` ) → `tsquery` Converts text to a `tsquery`, normalizing words according to the specified or default configuration. The words must be combined by valid `tsquery` operators.  `to_tsquery('english', 'The & Fat & Rats')` → `'fat' & 'rat'`  
`to_tsvector` ( [ _`config`_ `regconfig`, ] _`document`_ `text` ) → `tsvector` Converts text to a `tsvector`, normalizing words according to the specified or default configuration. Position information is included in the result.  `to_tsvector('english', 'The Fat Rats')` → `'fat':2 'rat':3`  
`to_tsvector` ( [ _`config`_ `regconfig`, ] _`document`_ `json` ) → `tsvector` `to_tsvector` ( [ _`config`_ `regconfig`, ] _`document`_ `jsonb` ) → `tsvector` Converts each string value in the JSON document to a `tsvector`, normalizing words according to the specified or default configuration. The results are then concatenated in document order to produce the output. Position information is generated as though one stopword exists between each pair of string values. (Beware that “document order” of the fields of a JSON object is implementation-dependent when the input is `jsonb`; observe the difference in the examples.)  `to_tsvector('english', '{"aa": "The Fat Rats", "b": "dog"}'::json)` → `'dog':5 'fat':2 'rat':3` `to_tsvector('english', '{"aa": "The Fat Rats", "b": "dog"}'::jsonb)` → `'dog':1 'fat':4 'rat':5`  
`json_to_tsvector` ( [ _`config`_ `regconfig`, ] _`document`_ `json`, _`filter`_ `jsonb` ) → `tsvector` `jsonb_to_tsvector` ( [ _`config`_ `regconfig`, ] _`document`_ `jsonb`, _`filter`_ `jsonb` ) → `tsvector` Selects each item in the JSON document that is requested by the _`filter`_ and converts each one to a `tsvector`, normalizing words according to the specified or default configuration. The results are then concatenated in document order to produce the output. Position information is generated as though one stopword exists between each pair of selected items. (Beware that “document order” of the fields of a JSON object is implementation-dependent when the input is `jsonb`.) The _`filter`_ must be a `jsonb` array containing zero or more of these keywords: `"string"` (to include all string values), `"numeric"` (to include all numeric values), `"boolean"` (to include all boolean values), `"key"` (to include all keys), or `"all"` (to include all the above). As a special case, the _`filter`_ can also be a simple JSON value that is one of these keywords.  `json_to_tsvector('english', '{"a": "The Fat Rats", "b": 123}'::json, '["string", "numeric"]')` → `'123':5 'fat':2 'rat':3` `json_to_tsvector('english', '{"cat": "The Fat Rats", "dog": 123}'::json, '"all"')` → `'123':9 'cat':1 'dog':7 'fat':4 'rat':5`  
`ts_delete` ( _`vector`_ `tsvector`, _`lexeme`_ `text` ) → `tsvector` Removes any occurrence of the given _`lexeme`_ from the _`vector`_. The _`lexeme`_ string is treated as a lexeme as-is, without further processing.  `ts_delete('fat:2,4 cat:3 rat:5A'::tsvector, 'fat')` → `'cat':3 'rat':5A`  
`ts_delete` ( _`vector`_ `tsvector`, _`lexemes`_ `text[]` ) → `tsvector` Removes any occurrences of the lexemes in _`lexemes`_ from the _`vector`_. The strings in _`lexemes`_ are taken as lexemes as-is, without further processing. Strings that do not match any lexeme in _`vector`_ are ignored.  `ts_delete('fat:2,4 cat:3 rat:5A'::tsvector, ARRAY['fat','rat'])` → `'cat':3`  
`ts_filter` ( _`vector`_ `tsvector`, _`weights`_ `"char"[]` ) → `tsvector` Selects only elements with the given _`weights`_ from the _`vector`_.  `ts_filter('fat:2,4 cat:3b,7c rat:5A'::tsvector, '{a,b}')` → `'cat':3B 'rat':5A`  
`ts_headline` ( [ _`config`_ `regconfig`, ] _`document`_ `text`, _`query`_ `tsquery` [, _`options`_ `text` ] ) → `text` Displays, in an abbreviated form, the match(es) for the _`query`_ in the _`document`_ , which must be raw text not a `tsvector`. Words in the document are normalized according to the specified or default configuration before matching to the query. Use of this function is discussed in [Section 12.3.4](textsearch-controls.md#TEXTSEARCH-HEADLINE "12.3.4. Highlighting Results"), which also describes the available _`options`_.  `ts_headline('The fat cat ate the rat.', 'cat')` → `The fat <b>cat</b> ate the rat.`  
`ts_headline` ( [ _`config`_ `regconfig`, ] _`document`_ `json`, _`query`_ `tsquery` [, _`options`_ `text` ] ) → `text` `ts_headline` ( [ _`config`_ `regconfig`, ] _`document`_ `jsonb`, _`query`_ `tsquery` [, _`options`_ `text` ] ) → `text` Displays, in an abbreviated form, match(es) for the _`query`_ that occur in string values within the JSON _`document`_. See [Section 12.3.4](textsearch-controls.md#TEXTSEARCH-HEADLINE "12.3.4. Highlighting Results") for more details.  `ts_headline('{"cat":"raining cats and dogs"}'::jsonb, 'cat')` → `{"cat": "raining <b>cats</b> and dogs"}`  
`ts_rank` ( [ _`weights`_ `real[]`, ] _`vector`_ `tsvector`, _`query`_ `tsquery` [, _`normalization`_ `integer` ] ) → `real` Computes a score showing how well the _`vector`_ matches the _`query`_. See [Section 12.3.3](textsearch-controls.md#TEXTSEARCH-RANKING "12.3.3. Ranking Search Results") for details.  `ts_rank(to_tsvector('raining cats and dogs'), 'cat')` → `0.06079271`  
`ts_rank_cd` ( [ _`weights`_ `real[]`, ] _`vector`_ `tsvector`, _`query`_ `tsquery` [, _`normalization`_ `integer` ] ) → `real` Computes a score showing how well the _`vector`_ matches the _`query`_ , using a cover density algorithm. See [Section 12.3.3](textsearch-controls.md#TEXTSEARCH-RANKING "12.3.3. Ranking Search Results") for details.  `ts_rank_cd(to_tsvector('raining cats and dogs'), 'cat')` → `0.1`  
`ts_rewrite` ( _`query`_ `tsquery`, _`target`_ `tsquery`, _`substitute`_ `tsquery` ) → `tsquery` Replaces occurrences of _`target`_ with _`substitute`_ within the _`query`_. See [Section 12.4.2.1](textsearch-features.md#TEXTSEARCH-QUERY-REWRITING "12.4.2.1. Query Rewriting") for details.  `ts_rewrite('a & b'::tsquery, 'a'::tsquery, 'foo|bar'::tsquery)` → `'b' & ( 'foo' | 'bar' )`  
`ts_rewrite` ( _`query`_ `tsquery`, _`select`_ `text` ) → `tsquery` Replaces portions of the _`query`_ according to target(s) and substitute(s) obtained by executing a `SELECT` command. See [Section 12.4.2.1](textsearch-features.md#TEXTSEARCH-QUERY-REWRITING "12.4.2.1. Query Rewriting") for details.  `SELECT ts_rewrite('a & b'::tsquery, 'SELECT t,s FROM aliases')` → `'b' & ( 'foo' | 'bar' )`  
`tsquery_phrase` ( _`query1`_ `tsquery`, _`query2`_ `tsquery` ) → `tsquery` Constructs a phrase query that searches for matches of _`query1`_ and _`query2`_ at successive lexemes (same as `<->` operator).  `tsquery_phrase(to_tsquery('fat'), to_tsquery('cat'))` → `'fat' <-> 'cat'`  
`tsquery_phrase` ( _`query1`_ `tsquery`, _`query2`_ `tsquery`, _`distance`_ `integer` ) → `tsquery` Constructs a phrase query that searches for matches of _`query1`_ and _`query2`_ that occur exactly _`distance`_ lexemes apart.  `tsquery_phrase(to_tsquery('fat'), to_tsquery('cat'), 10)` → `'fat' <10> 'cat'`  
`tsvector_to_array` ( `tsvector` ) → `text[]` Converts a `tsvector` to an array of lexemes.  `tsvector_to_array('fat:2,4 cat:3 rat:5A'::tsvector)` → `{cat,fat,rat}`  
`unnest` ( `tsvector` ) → `setof record` ( _`lexeme`_ `text`, _`positions`_ `smallint[]`, _`weights`_ `text` )  Expands a `tsvector` into a set of rows, one per lexeme.  `select * from unnest('cat:3 fat:2,4 rat:5A'::tsvector)` → ``
    
    
     lexeme | positions | weights
    --------+-----------+---------
     cat    | {3}       | {D}
     fat    | {2,4}     | {D,D}
     rat    | {5}       | {A}
      
  
  


### Note

All the text search functions that accept an optional `regconfig` argument will use the configuration specified by [default_text_search_config](runtime-config-client.md#GUC-DEFAULT-TEXT-SEARCH-CONFIG) when that argument is omitted. 

The functions in [Table 9.44](functions-textsearch.md#TEXTSEARCH-FUNCTIONS-DEBUG-TABLE "Table 9.44. Text Search Debugging Functions") are listed separately because they are not usually used in everyday text searching operations. They are primarily helpful for development and debugging of new text search configurations. 

**Table 9.44. Text Search Debugging Functions**

Function  Description  Example(s)   
---  
`ts_debug` ( [ _`config`_ `regconfig`, ] _`document`_ `text` ) → `setof record` ( _`alias`_ `text`, _`description`_ `text`, _`token`_ `text`, _`dictionaries`_ `regdictionary[]`, _`dictionary`_ `regdictionary`, _`lexemes`_ `text[]` )  Extracts and normalizes tokens from the _`document`_ according to the specified or default text search configuration, and returns information about how each token was processed. See [Section 12.8.1](textsearch-debugging.md#TEXTSEARCH-CONFIGURATION-TESTING "12.8.1. Configuration Testing") for details.  `ts_debug('english', 'The Brightest supernovaes')` → `(asciiword,"Word, all ASCII",The,{english_stem},english_stem,{}) ...`  
`ts_lexize` ( _`dict`_ `regdictionary`, _`token`_ `text` ) → `text[]` Returns an array of replacement lexemes if the input token is known to the dictionary, or an empty array if the token is known to the dictionary but it is a stop word, or NULL if it is not a known word. See [Section 12.8.3](textsearch-debugging.md#TEXTSEARCH-DICTIONARY-TESTING "12.8.3. Dictionary Testing") for details.  `ts_lexize('english_stem', 'stars')` → `{star}`  
`ts_parse` ( _`parser_name`_ `text`, _`document`_ `text` ) → `setof record` ( _`tokid`_ `integer`, _`token`_ `text` )  Extracts tokens from the _`document`_ using the named parser. See [Section 12.8.2](textsearch-debugging.md#TEXTSEARCH-PARSER-TESTING "12.8.2. Parser Testing") for details.  `ts_parse('default', 'foo - bar')` → `(1,foo) ...`  
`ts_parse` ( _`parser_oid`_ `oid`, _`document`_ `text` ) → `setof record` ( _`tokid`_ `integer`, _`token`_ `text` )  Extracts tokens from the _`document`_ using a parser specified by OID. See [Section 12.8.2](textsearch-debugging.md#TEXTSEARCH-PARSER-TESTING "12.8.2. Parser Testing") for details.  `ts_parse(3722, 'foo - bar')` → `(1,foo) ...`  
`ts_token_type` ( _`parser_name`_ `text` ) → `setof record` ( _`tokid`_ `integer`, _`alias`_ `text`, _`description`_ `text` )  Returns a table that describes each type of token the named parser can recognize. See [Section 12.8.2](textsearch-debugging.md#TEXTSEARCH-PARSER-TESTING "12.8.2. Parser Testing") for details.  `ts_token_type('default')` → `(1,asciiword,"Word, all ASCII") ...`  
`ts_token_type` ( _`parser_oid`_ `oid` ) → `setof record` ( _`tokid`_ `integer`, _`alias`_ `text`, _`description`_ `text` )  Returns a table that describes each type of token a parser specified by OID can recognize. See [Section 12.8.2](textsearch-debugging.md#TEXTSEARCH-PARSER-TESTING "12.8.2. Parser Testing") for details.  `ts_token_type(3722)` → `(1,asciiword,"Word, all ASCII") ...`  
`ts_stat` ( _`sqlquery`_ `text` [, _`weights`_ `text` ] ) → `setof record` ( _`word`_ `text`, _`ndoc`_ `integer`, _`nentry`_ `integer` )  Executes the _`sqlquery`_ , which must return a single `tsvector` column, and returns statistics about each distinct lexeme contained in the data. See [Section 12.4.4](textsearch-features.md#TEXTSEARCH-STATISTICS "12.4.4. Gathering Document Statistics") for details.  `ts_stat('SELECT vector FROM apod')` → `(foo,10,15) ...`  
  
  


* * *

[Prev](functions-net.md "9.12. Network Address Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")|  [Next](functions-uuid.md "9.14. UUID Functions")  
---|---|---  
9.12. Network Address Functions and Operators | [Home](index.md "PostgreSQL 17.5 Documentation")|  9.14. UUID Functions
