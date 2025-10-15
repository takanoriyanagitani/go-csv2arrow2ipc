#!/bin/sh

set -e

echo "Building csv2arrow2ipc..."
go build -o csv2arrow2ipc .

echo "Creating sample CSV data..."
cat <<EOF > sample.csv
a,b,c
1,2,3
4,5,6
EOF

echo "Running csv2arrow2ipc with sample data..."
cat sample.csv | ./csv2arrow2ipc -comma=, > sample.ipc

echo "Successfully created sample.ipc"

echo
echo "Showing the converted ipc file using arrow-cat..."
arrow-cat ./sample.ipc
