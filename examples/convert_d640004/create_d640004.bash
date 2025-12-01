#!/bin/bash 

for dir in /glade/campaign/collections/rda/data/d640004/*; do 
    echo $dir
    dir=`basename $dir`
    ../../src/create_kerchunk.py -d /glade/campaign/collections/rda/data/d640004/${dir} -a combine --regex "^.*\.nc$" -f "$dir.json" -mr -o /glade/campaign/collections/rda/work/rpconroy/ARCO/d640004/
done

