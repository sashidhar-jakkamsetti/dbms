cd rbf
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi
make clean
make
./rbftest1
./rbftest2
./rbftest3
./rbftest4
./rbftest5
./rbftest6
./rbftest7
./rbftest8
./rbftest8b
./rbftest9
./rbftest10
./rbftest11
./rbftest12
./rbftest_delete
./rbftest_update
./rbftest_p0
./rbftest_p1
./rbftest_p1b
./rbftest_p1c
./rbftest_p2
./rbftest_p2b
./rbftest_p3
./rbftest_p4
./rbftest_p5

make clean
