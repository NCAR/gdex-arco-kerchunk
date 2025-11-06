"""
Batch kerchunk creation for d640000.jra3q dataset

utilize the create_kerchunk.py script from 
https://github.com/NCAR/gdex-arco-kerchunk
to generate kerchunk reference json files

"""
import os
import glob
import logging
import subprocess
from gdex_work_utils import load_gdex_work_env, setup_logging

# Setup logging
log_file = setup_logging(__file__, log_name='batch_kerchunk.log')

# load environment variables
load_gdex_work_env()

# Loop through all subdirectories in the target directory and create kerchunk files
target_directory = os.path.join(os.environ['GLADE_DATA'], "d640000")
excluded_subdirs = ['catalog', 'kerchunk']

# locate the gdex-arco-kerchunk repo and create_kerchunk.py script
kerchunk_repo_path = os.path.join(os.environ['HOME_DIR'], "gdex-arco-kerchunk")
create_kerchunk_script = os.path.join(kerchunk_repo_path, "src/create_kerchunk.py")

subdirectories_full_path= glob.glob(f"{target_directory}/*")
for subdir_full_path in subdirectories_full_path:
    logging.info("------------------------------------------------")
    if os.path.basename(subdir_full_path) in excluded_subdirs:
        logging.info(f"Skipping excluded directory: {subdir_full_path}")
        continue
    subdir_full_path = os.path.normpath(subdir_full_path)
    # check if it's a directory
    if os.path.isdir(subdir_full_path):
        log_info = f"Processing directory: {subdir_full_path}"
        logging.info(log_info)

        # get only the subdirectory name
        subdir_name = os.path.basename(subdir_full_path)

        # check if kerchunk file already exists
        output_directory = os.path.join(os.environ['SCRATCH_DIR'], "d640000.jra3q/kerchunk")
        output_parquet_filename = f"{subdir_name}.parq"
        output_parquet_full_path = os.path.join(output_directory, output_parquet_filename)

        if os.path.exists(output_parquet_full_path):
            log_warn = f"Kerchunk file already exists for directory: {subdir_full_path}"
            logging.warning(log_warn)
            continue

        # define command
        kerchunk_command = [
            "python", create_kerchunk_script,
            "--action", "combine",
            "--directory", subdir_full_path,
            "--output_directory", output_directory,
            "--extensions", "nc",
            "--filename", output_parquet_filename,
            "--output_format", "parquet",
            "--cluster", "PBS",
            # "--dry_run",
            "--make_remote"
        ]

        # log the literal subprocessing command
        log_info = f"Subprocess command: {' '.join(kerchunk_command)}"
        logging.info(log_info)

        # run the command with error capture
        try:
            result = subprocess.run(
                kerchunk_command, 
                check=True,
                capture_output=True,
                text=True
            )
            # Log stdout if there's any
            if result.stdout:
                logging.info(f"Subprocess stdout: {result.stdout}")

        except subprocess.CalledProcessError as e:
            # Log the error details
            log_subprocess = f"Subprocess command failed: {' '.join(kerchunk_command)}"
            logging.error(log_subprocess)
            log_subprocess = f"Subprocess return code: {e.returncode}"
            logging.error(log_subprocess)
            if e.stdout:
                log_subprocess = f"Subprocess stdout: {e.stdout}"
                logging.error(log_subprocess)
            log_subprocess = f"Subprocess stderr: {e.stderr}"
            logging.error(log_subprocess)

            # print the error and skip to next directory
            continue

        log_info = f"Kerchunk file created for directory: {subdir_full_path}"
        logging.info(log_info)

logging.info("Batch kerchunk creation completed.")
