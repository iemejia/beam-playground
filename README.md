A simple playground for ideas for [Apache Beam](http://beam.apache.org/)

It also includes some Beam transforms / utils for debugging and other non-perfectly 
Beam aligned purposes:

- PTransform

Trace: Executes an element on a PCollection given a predicate (when).
Log: Traces every element on a PCollection.

TODO:

- PTransforms

Sort
Cache (not needed runner caches the data if you access it multiple times (BEAM-15)

- IO

ConsoleIO (bounded/unbounded)
KeyboardSource
SocketSource

- Spark specific

PCollection -> RDD
RDD -> PCollection
applyPTransform(rdd, ptransform)
