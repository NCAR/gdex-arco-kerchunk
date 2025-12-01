#!/bin/bash 

while read -r line; do
    regex=`echo $line | awk '{printf $1}'`
    filename=`echo $line | awk '{printf $2}'`
    echo $regex;
    echo $filename
    ../../src/create_kerchunk.py -d /gpfs/csfs1/collections/gdex/data/d633000/e5.oper.an.pl/ -a combine -of parquet --regex "^.*e5.oper.an.pl.*$regex.*.nc$" -f "${filename}" -mr -o pl633.0 
done < an.pl.txt

#for decade in {0..2}; do
#while read -r line; do
#    regex=`echo $line | awk '{printf $1}'`
#    filename=`echo $line | awk '{printf $2}'`
#    echo $regex;
#    echo $filename
#    ./create_kerchunk.py -d /gpfs/csfs1/collections/rda/data/ds633.0/e5.oper.an.pl/ -a combine --regex "^.*/20$decade.../.*e5.oper.an.pl.*$regex.*.nc$" -f "${filename}.20${decade}0-20${decade}9" -mr -o pl633.0 
#done < an.pl.txt
#done
#
