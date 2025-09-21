53.6. Message Data Types  
---  
[Prev](protocol-logical-replication.md "53.5. Logical Streaming Replication Protocol") | [Up](protocol.md "Chapter 53. Frontend/Backend Protocol")| Chapter 53. Frontend/Backend Protocol| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](protocol-message-formats.md "53.7. Message Formats")  
  
* * *

## 53.6. Message Data Types #

This section describes the base data types used in messages. 

Int _`n`_(_`i`_)
    

An _`n`_ -bit integer in network byte order (most significant byte first). If _`i`_ is specified it is the exact value that will appear, otherwise the value is variable. Eg. Int16, Int32(42). 

Int _`n`_[_`k`_]
    

An array of _`k`_ _`n`_ -bit integers, each in network byte order. The array length _`k`_ is always determined by an earlier field in the message. Eg. Int16[M]. 

String(_`s`_)
    

A null-terminated string (C-style string). There is no specific length limitation on strings. If _`s`_ is specified it is the exact value that will appear, otherwise the value is variable. Eg. String, String("user"). 

### Note

_There is no predefined limit_ on the length of a string that can be returned by the backend. Good coding strategy for a frontend is to use an expandable buffer so that anything that fits in memory can be accepted. If that's not feasible, read the full string and discard trailing characters that don't fit into your fixed-size buffer. 

Byte _`n`_(_`c`_)
    

Exactly _`n`_ bytes. If the field width _`n`_ is not a constant, it is always determinable from an earlier field in the message. If _`c`_ is specified it is the exact value. Eg. Byte2, Byte1('\n'). 

* * *

[Prev](protocol-logical-replication.md "53.5. Logical Streaming Replication Protocol") | [Up](protocol.md "Chapter 53. Frontend/Backend Protocol")|  [Next](protocol-message-formats.md "53.7. Message Formats")  
---|---|---  
53.5. Logical Streaming Replication Protocol | [Home](index.md "PostgreSQL 17.5 Documentation")|  53.7. Message Formats
