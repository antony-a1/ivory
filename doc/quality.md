
Current Quality Hitlist
-----------------------

  * Avoid internal type aliases, if there is a concept pull it out
  * Remove duplication, there is too much conceptual duplication in storage
  * Remove "hole" in the middle anti-pattern, composition first.
  * Configuration goes in as arguments. Remove mix of "configuration" styles with implicits and readers.
  * Consistent effect handling, unsafePerformIO's go at the top.
