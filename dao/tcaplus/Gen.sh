#! /bin/bash
# ############################################
# Filename: gen.sh
# Author: PIzhou
# Mail: yuanzhou@outlook.com
# CreateTime: 2021年04月06日 星期二 14时46分41秒
# ############################################

rm -fr tcaplus_uecqms
cd TdrCodeGen/
cp ../table_uecqms.xml .
python2 tdr.py table_uecqms.xml
mv tcaplus_uecqms ../


