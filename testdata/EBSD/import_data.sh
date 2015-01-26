#!/bin/bash
#python -m cProfile -o import_profile ../../ImportCSV/ImportCSV.py -t EBSD_network.csv -n WRZ_nodes.csv -l WRZ_links.csv -x -m ebsd_template.xml
python ../../ImportCSV/ImportCSV.py -t EBSD_network.csv -n EBSD_nodes.csv -l EBSD_links.csv -g EBSD_groups.csv -x -m ebsd_template.xml
