#!/bin/bash 
#fcst_mdl/  did not do
for dir in fcst_p/ fcst_p125/ fcst_phy2m/ fcst_phy2m125/ fcst_phy3m/ fcst_phyland/ fcst_phyland125/ fcst_phyp125/ fcst_snwlev/ fcst_surf/ fcst_surf125/ ll125_land/ ll125_surf/ minmax_surf/ tl479_land/ tl479_surf/*; do 
    echo $dir
    dir=`basename $dir`
    ../../src/create_kerchunk.py -d /glade/campaign/collections/rda/data/d640000/${dir} -a combine --regex "^.*\.nc$" -f "$dir.json" -mr -o /glade/campaign/collections/rda/work/rpconroy/ARCO/d640000/
done

