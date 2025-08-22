#!/bin/bash
rm -rf build/lingodb-debug/testdata/test
mkdir -p build/lingodb-debug/testdata/test
cat resources/sql/test/initialize.sql | build/lingodb-debug/sql build/lingodb-debug/testdata/test