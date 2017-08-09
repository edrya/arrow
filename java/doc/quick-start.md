## Introduction:
This is a "quick start" documentation for Arrow Java users. This documentation focuses on the basic usage of writing to and reading from Arrow data structure using Arrow Java library. This assumes the reader has some basic knowledge of Arrow, i.e., understand the concept of `Schema`, `Vector` and `RecordBatch`.

## ArrowRecordBatch
An `ArrowRecordBatch` is a collection of Arrow vectors of the same length. It can be think of "pure data", or a bunch of arrays.

## VectorSchemaRoot/FieldVector
`VectorSchemaRoot` is a `ArrowRecordBatch` holder and provides getters and setters to `ArrowRecordBatch`. `VectorSchemaRoot` is used as the main interface with other Arrow classes, such as `ArrowWriter` and `ArrowReader` (more on this later).  A `VectorSchemaRoot` can hold one `ArrowRecordBatch`.

`VectorSchemaRoot` is a collection of `FieldVector`s.

## VectorLoader/VectorUnloader
`VectorLoader`/ `VectorUnloader` are used to load/unload between `VectorSchemaRoot` and `ArrowRecordBatch`

## ArrowReader/ArrowWriter
`ArrowReader`/`ArrowWriter` are used to SeDer `VectorSchemaRoot` from/to binary format.
##

## Example Usage

### Write data to Arrow
First we write some data into a `VectorSchemaRoot`
```java
int[] data = new int[]
# Assume schema has a single integer field "v1"
VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)
FieldVector v1 = root.getVector("v1")
v1.allocateNew()
for (int i = 0; i < data.length; i++) {
    v1.getMutator().setSafe(i, data[i])
}
root.setRowCount(data.length)
```
Now we have the data in a `VectorSchemaRoot`, there are a few things we can do next:

**Turn it into a ArrowRecordBatch**:

We can use `VectorUnloader` to turn a `VectorSchemaRoot` to a `ArrowRecordBatch`
```
VectorUnloader unloader = new VectorUnloader(root)
ArrowRecordbatch recordBatch = unloader.getRecordBatch()
```
The `ArrowRecordBatch` can be passed to other parts of your program.

**Write the data out:**
We can use `ArrowStreamWriter` to write a `VectorSchemaRoot` to a `java.nio.channels.WritableByteChannel`:
```
WriteableByteChannel out = ...
ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)
writer.start()
writer.writeBatch()
writer.close()
```
Note that you can also stream data:
```
WriteableByteChannel out = ...
VectorSchemaRoot root = VectorSchemaRoot(schema, allocator)
ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)
writer.start()
// load some data into root
...
writer.writeBatch()

// load more data into root
...
writer.writeBatch()

// done writing
writer.close()
root.close()
```

Key methods:
allocateNew
mutater.setSafe
accessor.get*
reset
clear/close

## VectorSchemaRoot
This is a collection of FieldVectors

Key methods:
create(schema)
setRowCount()
close() // close all the field vectors and release the memory

## ArrowWriter
VectorSchemaRoot -> Bytes

## ArrowReader
Bytes -> VectorSchemaRoot

## MessageSerializer
Schema -> Bytes
Bytes -> Schema
ArrowRecordBatch -> Bytes
Bytes -> ArrowRecordBatch

## VectorLoader
**ArrowRecordBatch -> VectorSchemaRoot**
`VectorLoader` is used to "load" `ArrowRecordBatch` into `VectorSchemaRoot`

#### Key methods:
```java
public void load(ArrowRecordBatch recordBatch)
```

#### Example Usage:
```
VectorLoad loader = new VectorLoad(root)
loader.load(recordBatch)
```

## VectorUnloader
**VectorSchemaRoot -> ArrowRecordBatch**
`VectorUnloader` is used to "unload" data in a `VectorSchemaRoot` into `ArrowRecordBatch`

####  Key methods:
```java
public ArrowRecordBatch getRecordBatch()
```

####  Example Usage:
Unload all data in a vector schema root into a record batch
```java
VectorUnloader unloader = new VectorUnloader(root)
ArrowRecordBatch recordBatch = unloader.getRecordBatch()
```
