ulimit -t 120
cd ..
rm -rf stafftest-output-dir
(java -cp "lib/*:bin" ProjectMain $TESTDIR/../../../data/10_minutes stafftest-output-dir) > /dev/null 2>&1
diff stafftest-output-dir/part-r-00000 $TESTDIR/expected-output.txt