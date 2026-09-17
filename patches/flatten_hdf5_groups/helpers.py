
"""
   Provide different kerchunk concatenation strategies along new dimensions here. 
"""

import re
import ujson
import pprint


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


def get_groups_from_refs(ref):
    """
    Determine if a kerchunk reference (as an in-memory dict) contains
    groups (besides the root), and return their names.

    Parameters
    ----------
    ref : dict
        A kerchunk reference dict, e.g. the output of
        SingleHdf5ToZarr.translate() or MultiZarrToZarr.translate().
        Can be either the full dict (with a top-level "refs" key) or
        a bare refs dict.

    Returns
    -------
    list[str]
        Names (full paths) of all groups in the reference, excluding
        the root group itself. Empty list if the only group is root
        (i.e. everything is flat).
    """
    refs = ref.get("refs", ref)

    groups = []
    for key in refs:
        if key.endswith(".zgroup") and key != ".zgroup":
            group_name = key[: -len(".zgroup")].rstrip("/")
            groups.append(group_name)
    print(f"Groups found: {groups}")
    return sorted(groups)


def strip_all_group_prefixes(ref, groups):
    """
    Flatten a kerchunk reference dict by removing group structure entirely,
    moving all variables and coordinates to the root level.

    Parameters
    ----------
    ref : dict
        Kerchunk reference dict -- either the full dict (with a top-level
        "refs" key) or a bare refs dict.
    groups : list[str]
        Group paths to remove, e.g. ["Grid", "Grid/Intermediate"]. Include
        every group in the hierarchy you want flattened, however deep --
        the function does not infer nested groups on its own.

    Returns
    -------
    dict
        Same top-level shape as the input, with:
        - every array/chunk key stripped of its group prefix
        - _ARRAY_DIMENSIONS entries stripped of group prefixes to match
        - the removed groups' .zgroup entries dropped
        - any group-level .zattrs merged into the root .zattrs (later
          groups win on key collisions; check the result if group
          attrs matter to you)
    """
    is_full = isinstance(ref, dict) and "refs" in ref and isinstance(ref["refs"], dict)
    d = ref["refs"] if is_full else ref

    # longest first, so a key matching a nested group's full path is stripped
    # in one shot rather than partially
    sorted_groups = sorted(set(groups), key=len, reverse=True)
    group_zgroup_keys = {f"{g}/.zgroup" for g in sorted_groups}

    def strip_name(name):
        for g in sorted_groups:
            prefix = f"{g}/"
            if name.startswith(prefix):
                return name[len(prefix):]
        return name

    new_refs = {}
    root_attrs = None
    if ".zattrs" in d:
        val = d[".zattrs"]
        root_attrs = ujson.loads(val) if isinstance(val, str) else dict(val)

    for key, val in d.items():
        if key in group_zgroup_keys:
            continue  # this group no longer exists as a distinct entity

        new_key = strip_name(key)

        # group-level .zattrs (e.g. "Grid/.zattrs") collapse onto root ".zattrs"
        if new_key == ".zattrs" and key != ".zattrs":
            obj = ujson.loads(val) if isinstance(val, str) else val
            if root_attrs is None:
                root_attrs = {}
            root_attrs.update(obj)
            continue

        if new_key.endswith(".zattrs"):
            obj = ujson.loads(val) if isinstance(val, str) else val
            if "_ARRAY_DIMENSIONS" in obj:
                obj["_ARRAY_DIMENSIONS"] = [
                    strip_name(dim) for dim in obj["_ARRAY_DIMENSIONS"]
                ]
            val = ujson.dumps(obj)

        new_refs[new_key] = val

    if root_attrs is not None:
        new_refs[".zattrs"] = ujson.dumps(root_attrs)

    if is_full:
        ref["refs"] = new_refs
        return ref
    return new_refs
