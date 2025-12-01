#!/bin/bash 

for dir in /glade/campaign/collections/rda/data/d640000/*; do 
    echo $dir
    dir=`basename $dir`
    ../../src/create_kerchunk.py -d /glade/campaign/collections/rda/data/d640000/${dir} -a combine --regex "^.*\.nc$" -f "$dir.json" -mr -o /glade/campaign/collections/rda/work/rpconroy/ARCO/d640000/
done

