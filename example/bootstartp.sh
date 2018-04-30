#!/bin/bash
cd /home/guest/homework/hw2/part2/GraphLite/GraphLite-0.20/example
make
cd ..
start-graphlite example/Triangle.so ${GRAPHLITE_HOME}/part2-input/Triangle-graph0_4w ${GRAPHLITE_HOME}/out

