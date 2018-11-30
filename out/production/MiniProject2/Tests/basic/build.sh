#!/bin/bash
cd ..
mkdir -p bin
javac -cp "lib/*" src/*.java -d bin
