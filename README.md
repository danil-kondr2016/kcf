The KCF archiver project
========================

This repository contains the code of the KCF archiver project
with custom archive format, named "Kondratenko's Container File". 

The KCF specification is available [here](spec.md). 

> **Warning**
At this point neither specification nor API of KCF archiver 
has not been fixed since this project is in heavy alpha state.

TODO list
---------

### For KCF archive format version 1

- [ ] Chunk handling

- [ ] Utility for packing and unpacking KCF archives

- [ ] Compression and decompression

- [ ] Saving file metadata

- [ ] Data descriptors for unknown unpacked size and unpacked 
  data CRC32

### For KCF archive format version 2

- [ ] Multiple encoders/decoders (up to 3) as in 7-Zip file format

- [ ] Encryption for archive

- [ ] Solid archives

- [ ] Multi-volume archives

### For KCF archive format version 3

- [ ] Recovery records (some kind of Reed-Solomon codes or something 
like that)

  > **Note**
  It seems that there are some free and open source solutions for
  Reed-Solomon encoding, for example, Linux source code contains
  entire GPL library for
  it ([link](https://www.kernel.org/doc/html/latest/core-api/librs.html)). 
  I don't need to write it by myself (but I thought on that and have
  some doubts because I don't have any proper knowledge on that topic)





Licensing information
---------------------

This program product is to be licensed under conditions of GNU GPL v3.
