#! /bin/bash
# ############################################
# Filename: gen.sh
# Author: fairzhang
# Mail: fairzhang@tencent.com
# CreateTime: 2022年02月22日 
# ############################################

make
cd ./app/dispatch/main
make
cd ../.././node/main
make
