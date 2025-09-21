Chapter 8. Data Types  
---  
[Prev](queries-with.md "7.8. WITH Queries \(Common Table Expressions\)") | [Up](sql.md "Part II. The SQL Language")| Part II. The SQL Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](datatype-numeric.md "8.1. Numeric Types")  
  
* * *

## Chapter 8. Data Types

**Table of Contents**

[8.1. Numeric Types](datatype-numeric.md)
    

[8.1.1. Integer Types](datatype-numeric.md#DATATYPE-INT)
[8.1.2. Arbitrary Precision Numbers](datatype-numeric.md#DATATYPE-NUMERIC-DECIMAL)
[8.1.3. Floating-Point Types](datatype-numeric.md#DATATYPE-FLOAT)
[8.1.4. Serial Types](datatype-numeric.md#DATATYPE-SERIAL)
[8.2. Monetary Types](datatype-money.md)
[8.3. Character Types](datatype-character.md)
[8.4. Binary Data Types](datatype-binary.md)
    

[8.4.1. `bytea` Hex Format](datatype-binary.md#DATATYPE-BINARY-BYTEA-HEX-FORMAT)
[8.4.2. `bytea` Escape Format](datatype-binary.md#DATATYPE-BINARY-BYTEA-ESCAPE-FORMAT)
[8.5. Date/Time Types](datatype-datetime.md)
    

[8.5.1. Date/Time Input](datatype-datetime.md#DATATYPE-DATETIME-INPUT)
[8.5.2. Date/Time Output](datatype-datetime.md#DATATYPE-DATETIME-OUTPUT)
[8.5.3. Time Zones](datatype-datetime.md#DATATYPE-TIMEZONES)
[8.5.4. Interval Input](datatype-datetime.md#DATATYPE-INTERVAL-INPUT)
[8.5.5. Interval Output](datatype-datetime.md#DATATYPE-INTERVAL-OUTPUT)
[8.6. Boolean Type](datatype-boolean.md)
[8.7. Enumerated Types](datatype-enum.md)
    

[8.7.1. Declaration of Enumerated Types](datatype-enum.md#DATATYPE-ENUM-DECLARATION)
[8.7.2. Ordering](datatype-enum.md#DATATYPE-ENUM-ORDERING)
[8.7.3. Type Safety](datatype-enum.md#DATATYPE-ENUM-TYPE-SAFETY)
[8.7.4. Implementation Details](datatype-enum.md#DATATYPE-ENUM-IMPLEMENTATION-DETAILS)
[8.8. Geometric Types](datatype-geometric.md)
    

[8.8.1. Points](datatype-geometric.md#DATATYPE-GEOMETRIC-POINTS)
[8.8.2. Lines](datatype-geometric.md#DATATYPE-LINE)
[8.8.3. Line Segments](datatype-geometric.md#DATATYPE-LSEG)
[8.8.4. Boxes](datatype-geometric.md#DATATYPE-GEOMETRIC-BOXES)
[8.8.5. Paths](datatype-geometric.md#DATATYPE-GEOMETRIC-PATHS)
[8.8.6. Polygons](datatype-geometric.md#DATATYPE-POLYGON)
[8.8.7. Circles](datatype-geometric.md#DATATYPE-CIRCLE)
[8.9. Network Address Types](datatype-net-types.md)
    

[8.9.1. `inet`](datatype-net-types.md#DATATYPE-INET)
[8.9.2. `cidr`](datatype-net-types.md#DATATYPE-CIDR)
[8.9.3. `inet` vs. `cidr`](datatype-net-types.md#DATATYPE-INET-VS-CIDR)
[8.9.4. `macaddr`](datatype-net-types.md#DATATYPE-MACADDR)
[8.9.5. `macaddr8`](datatype-net-types.md#DATATYPE-MACADDR8)
[8.10. Bit String Types](datatype-bit.md)
[8.11. Text Search Types](datatype-textsearch.md)
    

[8.11.1. `tsvector`](datatype-textsearch.md#DATATYPE-TSVECTOR)
[8.11.2. `tsquery`](datatype-textsearch.md#DATATYPE-TSQUERY)
[8.12. UUID Type](datatype-uuid.md)
[8.13. XML Type](datatype-xml.md)
    

[8.13.1. Creating XML Values](datatype-xml.md#DATATYPE-XML-CREATING)
[8.13.2. Encoding Handling](datatype-xml.md#DATATYPE-XML-ENCODING-HANDLING)
[8.13.3. Accessing XML Values](datatype-xml.md#DATATYPE-XML-ACCESSING-XML-VALUES)
[8.14. JSON Types](datatype-json.md)
    

[8.14.1. JSON Input and Output Syntax](datatype-json.md#JSON-KEYS-ELEMENTS)
[8.14.2. Designing JSON Documents](datatype-json.md#JSON-DOC-DESIGN)
[8.14.3. `jsonb` Containment and Existence](datatype-json.md#JSON-CONTAINMENT)
[8.14.4. `jsonb` Indexing](datatype-json.md#JSON-INDEXING)
[8.14.5. `jsonb` Subscripting](datatype-json.md#JSONB-SUBSCRIPTING)
[8.14.6. Transforms](datatype-json.md#DATATYPE-JSON-TRANSFORMS)
[8.14.7. jsonpath Type](datatype-json.md#DATATYPE-JSONPATH)
[8.15. Arrays](arrays.md)
    

[8.15.1. Declaration of Array Types](arrays.md#ARRAYS-DECLARATION)
[8.15.2. Array Value Input](arrays.md#ARRAYS-INPUT)
[8.15.3. Accessing Arrays](arrays.md#ARRAYS-ACCESSING)
[8.15.4. Modifying Arrays](arrays.md#ARRAYS-MODIFYING)
[8.15.5. Searching in Arrays](arrays.md#ARRAYS-SEARCHING)
[8.15.6. Array Input and Output Syntax](arrays.md#ARRAYS-IO)
[8.16. Composite Types](rowtypes.md)
    

[8.16.1. Declaration of Composite Types](rowtypes.md#ROWTYPES-DECLARING)
[8.16.2. Constructing Composite Values](rowtypes.md#ROWTYPES-CONSTRUCTING)
[8.16.3. Accessing Composite Types](rowtypes.md#ROWTYPES-ACCESSING)
[8.16.4. Modifying Composite Types](rowtypes.md#ROWTYPES-MODIFYING)
[8.16.5. Using Composite Types in Queries](rowtypes.md#ROWTYPES-USAGE)
[8.16.6. Composite Type Input and Output Syntax](rowtypes.md#ROWTYPES-IO-SYNTAX)
[8.17. Range Types](rangetypes.md)
    

[8.17.1. Built-in Range and Multirange Types](rangetypes.md#RANGETYPES-BUILTIN)
[8.17.2. Examples](rangetypes.md#RANGETYPES-EXAMPLES)
[8.17.3. Inclusive and Exclusive Bounds](rangetypes.md#RANGETYPES-INCLUSIVITY)
[8.17.4. Infinite (Unbounded) Ranges](rangetypes.md#RANGETYPES-INFINITE)
[8.17.5. Range Input/Output](rangetypes.md#RANGETYPES-IO)
[8.17.6. Constructing Ranges and Multiranges](rangetypes.md#RANGETYPES-CONSTRUCT)
[8.17.7. Discrete Range Types](rangetypes.md#RANGETYPES-DISCRETE)
[8.17.8. Defining New Range Types](rangetypes.md#RANGETYPES-DEFINING)
[8.17.9. Indexing](rangetypes.md#RANGETYPES-INDEXING)
[8.17.10. Constraints on Ranges](rangetypes.md#RANGETYPES-CONSTRAINT)
[8.18. Domain Types](domains.md)
[8.19. Object Identifier Types](datatype-oid.md)
[8.20. `pg_lsn` Type](datatype-pg-lsn.md)
[8.21. Pseudo-Types](datatype-pseudo.md)

PostgreSQL has a rich set of native data types available to users. Users can add new types to PostgreSQL using the [CREATE TYPE](sql-createtype.md "CREATE TYPE") command. 

[Table 8.1](datatype.md#DATATYPE-TABLE "Table 8.1. Data Types") shows all the built-in general-purpose data types. Most of the alternative names listed in the “Aliases” column are the names used internally by PostgreSQL for historical reasons. In addition, some internally used or deprecated types are available, but are not listed here. 

**Table 8.1. Data Types**

Name| Aliases| Description  
---|---|---  
`bigint`| `int8`| signed eight-byte integer  
`bigserial`| `serial8`| autoincrementing eight-byte integer  
`bit [ (_`n`_) ]`|  | fixed-length bit string  
`bit varying [ (_`n`_) ]`| `varbit [ (_`n`_) ]`| variable-length bit string  
`boolean`| `bool`| logical Boolean (true/false)  
`box`|  | rectangular box on a plane  
`bytea`|  | binary data (“byte array”)  
`character [ (_`n`_) ]`| `char [ (_`n`_) ]`| fixed-length character string  
`character varying [ (_`n`_) ]`| `varchar [ (_`n`_) ]`| variable-length character string  
`cidr`|  | IPv4 or IPv6 network address  
`circle`|  | circle on a plane  
`date`|  | calendar date (year, month, day)  
`double precision`| `float8`| double precision floating-point number (8 bytes)  
`inet`|  | IPv4 or IPv6 host address  
`integer`| `int`, `int4`| signed four-byte integer  
`interval [ _`fields`_ ] [ (_`p`_) ]`|  | time span  
`json`|  | textual JSON data  
`jsonb`|  | binary JSON data, decomposed  
`line`|  | infinite line on a plane  
`lseg`|  | line segment on a plane  
`macaddr`|  | MAC (Media Access Control) address  
`macaddr8`|  | MAC (Media Access Control) address (EUI-64 format)  
`money`|  | currency amount  
`numeric [ (_`p`_ , _`s`_) ]`| `decimal [ (_`p`_ , _`s`_) ]`| exact numeric of selectable precision  
`path`|  | geometric path on a plane  
`pg_lsn`|  | PostgreSQL Log Sequence Number  
`pg_snapshot`|  | user-level transaction ID snapshot  
`point`|  | geometric point on a plane  
`polygon`|  | closed geometric path on a plane  
`real`| `float4`| single precision floating-point number (4 bytes)  
`smallint`| `int2`| signed two-byte integer  
`smallserial`| `serial2`| autoincrementing two-byte integer  
`serial`| `serial4`| autoincrementing four-byte integer  
`text`|  | variable-length character string  
`time [ (_`p`_) ] [ without time zone ]`|  | time of day (no time zone)  
`time [ (_`p`_) ] with time zone`| `timetz`| time of day, including time zone  
`timestamp [ (_`p`_) ] [ without time zone ]`|  | date and time (no time zone)  
`timestamp [ (_`p`_) ] with time zone`| `timestamptz`| date and time, including time zone  
`tsquery`|  | text search query  
`tsvector`|  | text search document  
`txid_snapshot`|  | user-level transaction ID snapshot (deprecated; see `pg_snapshot`)  
`uuid`|  | universally unique identifier  
`xml`|  | XML data  
  
  


### Compatibility

The following types (or spellings thereof) are specified by SQL: `bigint`, `bit`, `bit varying`, `boolean`, `char`, `character varying`, `character`, `varchar`, `date`, `double precision`, `integer`, `interval`, `numeric`, `decimal`, `real`, `smallint`, `time` (with or without time zone), `timestamp` (with or without time zone), `xml`. 

Each data type has an external representation determined by its input and output functions. Many of the built-in types have obvious external formats. However, several types are either unique to PostgreSQL, such as geometric paths, or have several possible formats, such as the date and time types. Some of the input and output functions are not invertible, i.e., the result of an output function might lose accuracy when compared to the original input. 

* * *

[Prev](queries-with.md "7.8. WITH Queries \(Common Table Expressions\)") | [Up](sql.md "Part II. The SQL Language")|  [Next](datatype-numeric.md "8.1. Numeric Types")  
---|---|---  
7.8. `WITH` Queries (Common Table Expressions) | [Home](index.md "PostgreSQL 17.5 Documentation")|  8.1. Numeric Types
