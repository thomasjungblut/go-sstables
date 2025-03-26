@0xbf8381dfd6a0d017;
using Go = import "/go.capnp";
$Go.package("test_files");
$Go.import("recordio/test_files");

struct TextLineCapnProto {
  lineNumber @0 :Int32;
  line @1 :Text;
}
