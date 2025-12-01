#!/bin/bash 

#for dir in /glade/campaign/collections/rda/data/d616000/hist3D/*; do 
for year in {1999..2020}; do
    echo $year
    echo '../../src/create_kerchunk.py -d /glade/campaign/collections/rda/data/d616000/hist2D/ -a combine --regex "^.*${year}.*wrf2d.*\.nc$" -f "$year.2d.json" -mr -o /glade/campaign/collections/rda/work/rpconroy/ARCO/d616000/'
    ../../src/create_kerchunk.py -d /glade/campaign/collections/rda/data/d616000/hist2D/ -a combine --regex "^.*wrf2d.*_${year}-.*\.nc$" -f "$year.2d.json" -mr -o /glade/campaign/collections/rda/work/rpconroy/ARCO/d616000/
done
