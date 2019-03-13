
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

cd ../rm
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi

make clean
make

./rmtest_create_tables
./rmtest_00
./rmtest_01
./rmtest_02
./rmtest_03
./rmtest_04
./rmtest_05
./rmtest_06
./rmtest_07
./rmtest_08
./rmtest_09
./rmtest_10
./rmtest_11
./rmtest_12
./rmtest_13
./rmtest_13b
./rmtest_14
./rmtest_15
./rmtest_extra_1
./rmtest_extra_2
./rmtest_p0
./rmtest_p1
./rmtest_p2
./rmtest_p3
./rmtest_p4
./rmtest_p5
./rmtest_p6
./rmtest_p7
./rmtest_p8
./rmtest_p9

make clean

cd ../ix
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi

make clean
make

./ixtest_01
./ixtest_02
./ixtest_03
./ixtest_04
./ixtest_05
./ixtest_06
./ixtest_07
./ixtest_08
./ixtest_09
./ixtest_10
./ixtest_11
./ixtest_12
./ixtest_13
./ixtest_14
./ixtest_15
./ixtest_extra_01
./ixtest_extra_02
./ixtest_p1
./ixtest_p2
./ixtest_p3
./ixtest_p4
./ixtest_p5
./ixtest_p6
./ixtest_pe_01
./ixtest_pe_02