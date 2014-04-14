Feature Encodings
=================

Overview
--------

Ivory stores EAVT facts. The supported value types are currently:

* int
* long
* double
* boolean
* string
* tombstone

In addition to these types, there are use cases where storing facts with *complex* value types
would be beneficial. For example, storing a fact representing a purchase. The value would be a
richer type containing a string for the product ID, an int for the purchase price, etc.


Implementation
--------------

There are a couple of options here:

* Introducing a *byte array* value type to facts. Any rich type would always be serialised to
a byte array and it would be the responsibility of the reader/query to apply a compatible dictionary
encoding such that the value can be deserialised correctly.

* Make the internal representation of values always byte arrays. This puts the responsibility of
correct schema application on to the reader/query.

* Store the write-schema along with the value.


Schema Evolution
----------------

Support for schema evolutions probably fits in here somewhere.
