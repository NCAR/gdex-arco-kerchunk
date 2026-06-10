
The following metadata could be scrubbed from the ensemble reference files, since they are appropriate for single ensemble members only.
They are inherited through the MultiZarrToZarr functionality, which sets all global metadata from the first concatenated file.  

def postprocess_ensemble(ref):
    ref_ = zarr.open(ref)
    # Delete metadata fields meant for individual ensemble members
    delete_fields = ['realization_index', 'realization', 'experiment_id', 'experiment', 'history', 'variant_label', 
                     'further_info_url', 'tracking_id', 'original_file_names', 'original_file_hash_codes',
                     'branch_time', 'parent_time_units', 'parent_variant_label', 'parent_experiment_rip', 
                     'physics_index', 'forcing_index', 'initialization_index', 'branch_time_in_parent',
                     'branch_time_in_child', 
                    ]
    for field in delete_fields:
        if ref_.attrs.get(field, None):
            del ref_.attrs[field]
    return ref

