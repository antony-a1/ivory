Dates and Formats
=================

Dates are bad. So it is really important that ivory keep a coherent view of
dates, times and timezones. However, traditional approaches for this (such
as always storing data in UTC and converting in/out) have potential drawbacks
in a system like ivory. Three things that we need to be aware of are:

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

  * An ivory repository operates in a single local timezone ID, i.e. the
    "repository timezone". The preference should be for the repository timezone
    to be a locale dependent timezone such as "Australia/Sydney" to account
    for daylight savings and other locale specific events, without unecessary
    overhead. Only use an offset timezone if the intent really is for the
    repository to be local independent such as the UTC everywhere model,
    however if you do this you will need to manage conversions to and from dates
    externally to ivory as required.

  * All data stored by ivory is stored in the repository timezone.

  * Facts that have dates without times are valid as at 00:00:00 for the day
    specified in the repository timezone.

  * __No__ date without a time shall be imported in any other zone, and no
    transformation shall occur to account for timezones on that day.


Guidance
--------

  * The _best_ timezone for your repository, is the timezone where the most facts
    of _day_ granularity occur.


Justification
-------------

  * By operating ivory in a local time zone, it allows significant storage
    savings for the common "day only" case, where we remove the need to store
    any information about time for each fact.

  * By operating in a local time zone, we remove the overhead of additional
    processing of dates for the common case, i.e. data from the local zone.

  * By only storing data in a single time zone (as opposed to a timezone
    per fact set or other granularities etc...) we are able to incur all
    translation costs on ingestion.

  * As outlined, it is possible to use the UTC everywhere model, by just
    configuring the repository timezone to be `UTC`. All other processes
    work as required - however it is worth noting that all data not in
    UTC will need to include time information.


Formats
-------

Ivory supports a sub-set of ISO 8601 timestamps.

#### Local Date Only

 `yyyy-MM-dd` -

    Date with a day granularity in the local time zone. Example: `2012-01-15`,
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

### Data of Day Granularity from multiple time-zones

#### `Scenario`

  You have daily loads from sites across different timezones.

```
Brisbane Facts, +1000
---------------------
E1|A1|1|2010-03-03
E2|A1|2|2010-03-03

Sydney Facts, +1100
-------------------
E3|A1|3|2010-03-03
E4|A1|3|2010-03-03

London Facts, +0000
-------------------
E5|A1|5|2010-03-03
E6|A1|6|2010-03-03

Ivory as +1100
--------------
???

```


##### `Ingestion Solution 1`

  For each load decide what "day" it really occurs on. This may be
  that you actually want it to occur on "same" day as repository
  timezone or offset by one. In our example we decide Brisbane and
  Sydney facts occur on the same day, London facts actually occur
  the day after (note well, it is likely domain specific knowledge
  will likely be required to work this out), so we process this
  into a different data set.


```
Brisbane Facts, +1000
---------------------
E1|A1|1|2010-03-03
E2|A1|2|2010-03-03

Sydney Facts, +1100
-------------------
E3|A1|3|2010-03-03
E4|A1|3|2010-03-03

London Facts, +0000
-------------------
E5|A1|5|2010-03-04
E6|A1|6|2010-03-04

```

  Note that the date of london has been modified. We now then import
  these file as if they were in the repository time zone and end up
  with:

```
Ivory as +1100
--------------
E1|A1|1|2010-03-03T00:00:00+11:00
E2|A1|2|2010-03-03T00:00:00+11:00
E3|A1|3|2010-03-03T00:00:00+11:00
E4|A1|3|2010-03-03T00:00:00+11:00
E5|A1|5|2010-03-04T00:00:00+11:00
E6|A1|6|2010-03-04T00:00:00+11:00
```

##### `Ingestion Solution 2`

  If the time is critical to the facts it is possible that you want
  to maintain the time difference between each of these, to do this
  we preprocess each data set into a date-time format. For example:


```
Brisbane Facts, +1000
---------------------
E1|A1|1|2010-03-03T00:00:00+10:00
E2|A1|2|2010-03-03T00:00:00+10:00

Sydney Facts, +1100
-------------------
E3|A1|3|2010-03-03T00:00:00+11:00
E4|A1|3|2010-03-03T00:00:00+11:00

London Facts, +0000
-------------------
E5|A1|5|2010-03-04T00:00:00+00:00
E6|A1|6|2010-03-04T00:00:00+00:00
```

  We then import as per normal, and the import process will translate
  the times as required.

```
Ivory as +1100
--------------
E1|A1|1|2010-03-03T01:00:00+11:00
E2|A1|2|2010-03-03T01:00:00+11:00
E3|A1|3|2010-03-03T00:00:00+11:00
E4|A1|3|2010-03-03T00:00:00+11:00
E5|A1|5|2010-03-03T11:00:00+11:00
E6|A1|6|2010-03-03T11:00:00+11:00
```


### Data sourced from a number of time-zones

##### `Scenario`
  You have sales data from sites across different timezones.

```
Brisbane Facts, +1000
---------------------
E1|A1|1|2010-03-03T09:30:00
E2|A1|2|2010-03-03T14:30:00

Sydney Facts, +1100
-------------------
E3|A1|3|2010-03-03T10:30:00
E4|A1|3|2010-03-03T14:30:00

Ivory as +1100
--------------
???

```

##### `Ingestion Solution 1`
  Preprocess all data to include timezone information
  on a per-row basis using the "Locale independent" format.

```
Brisbane Facts, +1000
---------------------
E1|A1|1|2010-03-03T09:30:00+10:00
E2|A1|2|2010-03-03T14:30:00+10:00

Sydney Facts, +1100
-------------------
E3|A1|3|2010-03-03T10:30:00+11:00
E4|A1|3|2010-03-03T14:30:00+11:00

Ivory as +1100
--------------
E1|A1|1|2010-03-03T10:30:00+11:00
E2|A1|2|2010-03-03T15:30:00+11:00
E3|A1|3|2010-03-03T10:30:00+11:00
E4|A1|3|2010-03-03T14:30:00+11:00
```

##### `Ingestion Solution 2`
  Perform individual ingestions for each timezone, using the
  "Local date / time" format, but specify an overriding
  ingestion timezone for the whole dataset. The ingestion
  will then translate each row into the repository timezone.

```
Brisbane Facts, +1000
---------------------
E1|A1|1|2010-03-03T09:30:00
E2|A1|2|2010-03-03T14:30:00

Sydney Facts, +1100
-------------------
E3|A1|3|2010-03-03T10:30:00
E4|A1|3|2010-03-03T14:30:00

Ivory as +1100
--------------
E1|A1|1|2010-03-03T10:30:00+11:00
E2|A1|2|2010-03-03T15:30:00+11:00
E3|A1|3|2010-03-03T10:30:00+11:00
E4|A1|3|2010-03-03T14:30:00+11:00
```


Daylight Savings Time
---------------------

Some extra treatment for daylight savings time.

Facts are stored with milliseconds since the start of the day. On
daylight savings cross over days where an hour is _gained_ there is an
overlap where multiple hour / minutes all map to the same "second of
day" measure used by most date / time libraries (for example
joda). Currently ivory handles this in the same way where "second of
day" is computed only from the hour, minute and second - independent
of DST related timezone offset changes.

To address this we could do one of two things:
   - annotate DST overlapped hours with an extra bit in the time field; or
   - offset time by an additional interval to handle the gained time.

However, both of these things require non-standard treatment of "second
of day" and will require code changes to ivory to handle.

To be clear, at this point ivory handles "second of day" based only on
hour, minute and second of day. But in the future further information
may be added to provide additional mechanisms to deal with DST
overlaps.


Current Status
--------------

There are number of key pieces of this which are not complete:

  - Date formats and parsing are spread throughout the codebase, so
    there is no "standard library" for dealing with dates in a
    consistent way within ivory.

  - The ISO 8601 variants are not complete and not uniformly
    supported.

  - Ingestion incorrectly forces a timezone to be specified for
    an input data set. A better default would be to only allow
    the above spec'd date/time formats, but allow the ingestion
    process to override the timezone for an entire data-set,
    where every row is converted. Note that this may have
    performance overhead compared with the local only approach.

  - Ivory does not store the timezone of the repository on creation.
    This means it could be incorrectly impacted by system changes.
