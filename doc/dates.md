Dates and Formats
=================

Dates are bad. So it is really important that ivory keep a coherent view of
dates, times and timezones. However, traditional approaches for this (such
as always storing data in UTC and converting in/out) have potential drawbacks
in a system like ivory, three things that we need to be aware of are:

 * Storage has a non-trivial cost, so anything that adds significant per-fact
   storage overhead needs to be avoided.

 * Processing has a non-trivial cost, so anything that adds significant per-fact
   processing overhead needs to be avoided.

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


Guidance
--------

  * The _best_ timezone for you data-set, is the timezone where the most facts
    of _day_ granularity occurs.


Justification
-------------

  * By operating ivory in a local time zone, it allows significant storage
    savings for the common "day only" case, where we remove the need to store
    any information about time for each fact.

  * By operating in a local time zone, we remove the overhead of additional
    processing of dates on ingestion for the common case, i.e. data from
    the local zone.

  * By only storing data in a single time zone (as opposed to a timezone
    per fact set or other granularities etc...) we are able to incur all
    translation costs on ingestion.

  * As outlines, it is possible to use the UTC everywhere model, by just
    configuring the repository timezone to be `UTC`. All other processes
    work as required - however it is worth noting that all data not in
    UTC will need to include time information.


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


Larger Examples
---------------

### Data sourced from a number of time-zones

`Scenario`:
  You have sales data from sites across different timezones.

`Ingestion Solution 1`:
  Preprocess all data to include timezone information
  on a per-row basis using the "Locale independent" format.

`Ingestion Solution 2`:
  Perform individual ingestions for each timezone, using the
  "Local date / time" format, but specificy an overriding
  ingestion timezone for the whole dataset. The ingestion
  will then translate each row into the Ivory timezone.

`Extraction Solution 1`:
  Specify all queries in the Ivory timezone.

`Extraction Solution 2`:
  If there is a need for more fine grained extraction dates,
  use the "Locale independent" format as required.


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
    the above spec'd date/time formats, but allow the ingestion
    process to override the timezone for an entire data-set,
    where every row is converted. Note that this may have
    performance overhead compared with the local only approach.

  - Ivory does not store the timezone of the repository on creation.
    This means it could be incorrectly impacted by system changes.
