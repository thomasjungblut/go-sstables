## Using Kaitai Struct

The best about Kaitai is its excellent documentation. You can find how to install and use it from their [website](https://kaitai.io/#quick-start). Below we make a quick example on how to compile and use it in Java, other languages work the same way.

All we need here is the `ksc` command (or `kaitai-struct-compiler`), the respective `ksy` file and the target of compilation. 

Here, for example, we compile for Java:

> ksc recordio_v4.ksy --target java

In the case of recordio v4 we need receive two files: one for the vint compression schema and one for the actual recordio files.

The Java code is fairly easy to use, you would just need to run:

```java
RecordioV2 records = RecordioV2.fromFile("/some/record.io");
for(Record r : records.record()) {
  ...
}
```
