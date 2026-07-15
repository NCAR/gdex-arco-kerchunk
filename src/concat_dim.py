
"""
   Provide different kerchunk concatenation strategies along new dimensions here. 
"""

import re


def get_ensemble(files):
    """
        From a list of CMIP filenames, extract and return the ensemble member IDs.

        Note that the MMLEA2 dataset has some strange conventions for ensemble member ID formats.
    """
    #  This should be the standard expression for CMIP data:
    #  member_searches = [re.search(r"r(\d+)i(\d+)p(\d+)(f(\d+))?", file) for file in files]

    # In MMLEA2, CESM2 LENS data have an optional "r" parameter and can have float values for first parameter.  
    member_searches = [re.search(r"r?(\d+)(\.\d+)?i(\d+)p(\d+)(f(\d+))?", file) for file in files]

    assert all(member_searches), "Not all ensemble IDs were found in the given files"
    member_ids = [member.group() for member in member_searches]
    print("Ensemble member ids: " + (", ".join(member_ids)))
    # Assert ensemble members are nonempty and unique
    assert all(member_ids), "List contains empty strings"
    assert len(member_ids) == len(set(member_ids)), "List contains duplicates"
    return member_ids