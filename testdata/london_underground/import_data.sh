#!/bin/bash
#python -m cProfile -o import_profile ../../ImportCSV/ImportCSV.py -t EBSD_network.csv -n WRZ_nodes.csv -l WRZ_links.csv -x -m ebsd_template.xml
python ../../ImportCSV/ImportCSV.py -t network.csv -x -m template/template.xml
