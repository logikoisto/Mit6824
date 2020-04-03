#!/bin/bash
echo "mr testing..."
echo "==> Part I"
go test -run Sequential ../../mapreduce/...
echo ""
echo "==> Part II"
(cd ./wordcount && sh test-wc.sh > /dev/null)
echo ""
echo "==> Part III"
go test -run TestParallel ../../mapreduce/...
echo ""
echo "==> Part IV"
go test -run Failure ../../mapreduce/...
echo ""
echo "==> Part V (inverted index)"
(cd ./invertedindex && sh test-ii.sh > /dev/null)

rm ./invertedindex/mrtmp.* ./invertedindex/diff.out
rm ./wordcount/mrtmp.* ./wordcount/diff.out
