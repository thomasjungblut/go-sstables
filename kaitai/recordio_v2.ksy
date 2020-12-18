meta:
  id: recordio_v2
  endian: le
  imports:
    - vlq_base128_le
seq:
  - id: file_header
    type: file_header
  - id: record
    type: record
    repeat: eos
types:
  file_header:
   doc: |
     recordio header format to figure out the version it was written and whether the records are compressed.
   seq:
    - id: version
      type: u4
      doc: The version of the recordio format used in this file.
    - id: compression_type
      type: u4
      enum: compression
      doc: The compression algorithm used. 0 means no compression, 1 means Snappy, 2 means Gzip.
  record:
    doc: |
      recordio record is an "infinite" stream of magic number separated and length encoded byte arrays.
    seq:
      - id: magic
        contents: [0x91, 0x8D, 0x4C]
      - id: uncompressed_payload_len
        type: vlq_base128_le
      - id: compressed_payload_len
        type: vlq_base128_le
      - id: payload
        size: record_size
    instances:
      record_size:
        value: uncompressed_payload_len.value ^ compressed_payload_len.value
        doc: The size is either the compressed or uncompressed length.
enums:
  compression:
    0: none
    1: snappy
    2: gzip