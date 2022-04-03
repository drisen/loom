# loom
A simple map-reduce implemented in concurrent threads.

**loom**(producer, mapfunc, consumer)
 Distributes items from a producer Generator in a new thread through a farm of
  created mapping-function threads to a consumer in the calling thread.
