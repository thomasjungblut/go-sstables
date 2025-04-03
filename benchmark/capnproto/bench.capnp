@0xbf8381dfd6a0d017;
using Go = import "/go.capnp";
$Go.package("capnproto");
$Go.import("benchmark/capnproto");

struct BytesMsg {
  key @0 :Data;
}
