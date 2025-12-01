#!/bin/bash 

for dir in /glade/campaign/collections/rda/data/d559000/wy1997 /glade/campaign/collections/rda/data/d559000/wy1998 /glade/campaign/collections/rda/data/d559000/wy1999 /glade/campaign/collections/rda/data/d559000/wy1990; do 
    echo $dir
    dir=`basename $dir`
    ../../src/create_kerchunk.py -d /glade/campaign/collections/rda/data/d559000/${dir} -a combine --regex "^.*wrf3d.*\.nc$" -f "$dir.3d.json" -mr -o /glade/campaign/collections/rda/work/rpconroy/ARCO/d559000/
done
