First-class feature generation
==============================

Specification for adding first-class support for feature generation into
Ivory.


High level steps
----------------

1. Improve dictionary
  * thrift
  * versioning
  * migration tooling
  * update import-dictionary to take thrift structs

2. Add support for complex encodings for facts
  * a way to describe it in the dictionary
    * 'struct' here means a "named tuple", i.e. a product type
    * likely representation would be array of Values
    * also support arrays of values in general
  * dictionary 'encoding' format
  * "fact" representation
  * "import"/"export" representation

3. Add support for windows
  * Add explicit "window" argument in dictionary, e.g. 'latest', 'x-days', 'x-months'
  * Expose "reduce" function for extract, e.g. (e, a, [(v, t)]) => (e, a, v, t)

4. Add support for Meta features
  * Alias "real" features or parts-of

5. Add support for Set features
  * Define operation in dictionary, e.g. avg, min, max, latest, count, sum, histogram
  * Map those to customer "reduce" functions that get called first
  * Define over keys of structs

6. Scalar combination of features
  * This is essentially a "row-level" operation
  * Supporting this also means opening the door to allow scoring to be supported on
  top of Ivory
