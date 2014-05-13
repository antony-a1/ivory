Dates and Formats
=================

Dates are bad. So it is really important that ivory keep a coherent view of
dates, times and timezones. However, traditional approaches for this (such
as always storing data in UTC and converting in/out) have severe drawbacks
in a system like ivory, two things that we need to be accutely aware of are:

 * Storage has a non-trivial cost, so anything that adds significant per-fact
   storage overhead needs to be avoided.

 * Ivory should handle data of various input qualities, and it is a common
   scenario for source data to have date level of granularity with no time
   information. We should not "make-up" information to fill in the gap if
   that information could be wrong or lead to data quality issues.


Over-arching Rules
------------------

  * An ivory repository operates in a single local timezone ID. This timezone
    should be a locale dependent timezone such as "Australia/Sydney" to account
    for daylight savings and other locale specific events. Only use an offset
    timezone if the intent really is for the repository to be local independent
    such as the UTC everywhere model, however if you do this you will need to
    manage conversions to and from dates externally to ivory as required.

  * All data stored by ivory is in the local timezone.

  * Facts that have dates without times will be stored in that way, and will
    be considered valid for the _entire_ day.


Formats
-------

Ivory supports a sub-set of ISO 8601 timestamps.

#### Local Date Only

 `yyyy-MM-dd` -
    Date with a day granularitry in the local time zone. Example: `2012-01-15`,
    `2014-12-31`.

#### Local Date And Time

 `yyyy-MM-ddThh:mm:ss` -
   Date/Time with a second granularity in the local time zone. Example:
   `2012-01-15T16:00:31`,  `2014-12-31T00:01:01`.

#### Locale independent Date And Time

 `yyyy-MM-ddThh:mm:ss[+|-]hh:mm` -
   Date/Time with a second granularity in a specific time zone. Ivory will
   translate this to the local timezone to store data and before performing
   queries. Note that this conversion may have _significant_ performance
   overhead so only use when necessary. Example: `2012-01-15T16:00:31+11:00`,
   `2014-12-31T00:01:01-03:10`.

#### Other Formats

 `yyyy-MM-dd hh:mm:ss` -
   Date/Time with a second granularity in the local time zone. Example:
   `2012-01-15 16:00:31`,  `2014-12-31 00:01:01`. This is equivalent to the
    Local Date and Time format, and will be deprecated at some point in
    the future (primarily to stick with ISO 8601 as a unifying standard
    for all formats).


Current Status
--------------

There are number of key pieces of this which are not complete:

  - Date formats and parsing are spread throughout the codebase, so
    there is no "standard library" for dealing with dates in a
    consistent way within ivory.

  - The ISO 8601 variants are not complete and not uniformally
    supported.

  - Ingestion incorrectly forces a timezone to be specified for
    an input data set. A better default would be to only allow
    the above spec'd date/time formats, but if required ingestion
    could allow an override for an entire data-set - but note that
    this will have performance issues other the alternatives.

  - Ivory does not store the timezone of the repository on creation.
    This means it could be incorrectly impacted by system changes.
