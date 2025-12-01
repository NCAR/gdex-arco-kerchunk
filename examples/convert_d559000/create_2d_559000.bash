#!/bin/bash 

for dir in /glade/campaign/collections/rda/data/d559000/*; do 
    echo $dir
    dir=`basename $dir`
    ../../src/create_kerchunk.py -d /glade/campaign/collections/rda/data/d559000/${dir} -a combine --regex "^.*wrf2d.*\.nc$" -f "$dir.2d.json" -mr -o /glade/campaign/collections/rda/work/rpconroy/ARCO/d559000/
done
