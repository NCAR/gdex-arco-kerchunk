#!/bin/bash 

#for dir in /glade/campaign/collections/rda/data/d616000/hist3D/*; do 
for year in {1999..2020}; do
    echo $year
    echo '../../src/create_kerchunk.py -d /glade/campaign/collections/rda/data/d616000/hist3D/ -a combine --regex "^._*${year}.*wrf3d.*\.nc$" -f "$year.3d.json" -mr -o /glade/campaign/collections/rda/work/rpconroy/ARCO/d616000/'
    ../../src/create_kerchunk.py -d /glade/campaign/collections/rda/data/d616000/hist3D/ -a combine --regex "^.*wrf3d.*_${year}-.*\.nc$" -f "$year.3d.json" -mr -o /glade/campaign/collections/rda/work/rpconroy/ARCO/d616000/
done
