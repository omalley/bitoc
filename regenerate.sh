#!/bin/bash
cd src/main/flatbuffer
flatc -j -o ../java --gen-mutable bitoc.fbs
