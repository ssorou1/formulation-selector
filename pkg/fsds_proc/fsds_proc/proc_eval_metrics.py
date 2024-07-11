'''
Helper functions for processing evaluation metrics datasets
author:: Guy Litt <guy.litt@noaa.gov>
description:: functions read in yaml schema and standardize metrics datasets
notes:: developed using python v3.12

Changelog/contributions
    2024-07-02 Originally created, GL
    2024-07-09 added different file format/dir path options; add file format checkers, GL

'''


import pandas as pd
from pathlib import Path
import yaml
import xarray as xr
import netCDF4
import warnings
import os

def _proc_flatten_ls_of_dict_keys(config: dict, key: str) -> list:
    keys_cs = list()
    for v in config[key]:
        keys_cs.append(list(v.keys()))
    return [x for xs in keys_cs for x in xs]

def _proc_check_input_config(config: dict, std_keys=['file_io','col_schema','formulation_metadata','references'],
                             req_col_schema=['gage_id', 'metric_cols'],
                             req_form_meta=['dataset_name','formulation_base','target_var','start_date', 'end_date','cal_status'],
                             req_file_io=['dir_save', 'save_type','save_loc']):
    
    """
    Check input config file to ensure it contains the minimum expected categories

    :raises ValueError: _description_
    :raises ValueError: _description_
    :raises ValueError: _description_
    :raises ValueError: _description_

    seealso:: :func: `read_schm_ls_of_dict`
    :TODO: add further checks after testing more datasets

    """
    # Expected standard keys:
    chck_dict = {key: config[key] for key in std_keys}
    if len(chck_dict) != len(std_keys):
        raise ValueError(f'The provided keys in the input config file should include the following: {", ".join(std_keys)}')
    
    # required keys defined inside col_schema
    keys_col_schema = _proc_flatten_ls_of_dict_keys(config, 'col_schema')
    if not all([x in keys_col_schema for x in req_col_schema]):
        raise ValueError(f"The input config file expects the following defined under 'col_schema': {', '.join(req_col_schema)}")

    # required keys defined in formulation_metadata
    keys_form_meta = _proc_flatten_ls_of_dict_keys(config, 'formulation_metadata')
    if not all([x in keys_form_meta for x in req_form_meta]):
        raise ValueError(f"The input config file expects the following defined under 'formulation_metadata': {', '.join(req_form_meta)}")

    # required keys defined in file_io
    keys_file_io = _proc_flatten_ls_of_dict_keys(config, 'file_io')
    if not all([x in keys_file_io for x in req_file_io]):
        raise ValueError(f"The input config file expects the following defined under 'formulation_metadata': {', '.join(req_file_io)}")

def read_schm_ls_of_dict(schema_path: str | os.PathLike) -> pd.DataFrame:
    """Read a dataset's schema file designed as a list of dicts

    :param schema_path: _description_
    :type schema_path: str | os.PathLike
    :return: he filepath to the schema
    :rtype: pd.DataFrame
    note::
    Changelog/contributions
        2024-07-02 Originally created, GL
    """
    # Load the YAML configuration file
    with open(schema_path, 'r') as file:
        config = yaml.safe_load(file)

    # Run check on expected config formats
    _proc_check_input_config(config)

    # Convert dict of lists into pd.DataFrame
    ls_form = list()
    for k, vv in config.items():

        for v in vv:
            ls_form.append(pd.DataFrame(v, index = [0]))
    df_all = pd.concat(ls_form, axis=1)

    return df_all

def _save_dir_struct(dir_save: str | os.PathLike, dataset_name: str, save_type:str ) -> tuple[Path, dict]:
    # Create a standard directory saving structure (in cases of local filesaving)
    save_dir_base = Path(Path(dir_save) / Path('user_data_std') / dataset_name)
    save_dir_base.mkdir(exist_ok=True, parents = True)

    other_save_dirs = dict()
    if save_type == 'csv' or save_type == 'parquet': # For non-hierachical files
        # Otherwise single hierarchical files will be saved in lieu of subdirectories populated w/ .csv files

        # Design dir structure for writing multiple files
        save_dir_attr = Path(save_dir_base / Path('attributes'))
        save_dir_eval_metr = Path(save_dir_base / Path('eval')/Path('metrics'))
        save_dir_eval_ts = Path(save_dir_base / Path('eval')/Path('timeseries'))
        save_dir_meta = Path(save_dir_base / Path('metadata'))
        save_dir_meta_lic = Path(save_dir_meta/Path('license'))
        save_dir_config =  Path(save_dir_base / Path('config'))
        # Generate the expected subdirectories for storing multiple files
        save_dir_attr.mkdir(exist_ok=True, parents = True)
        save_dir_eval_metr.mkdir(exist_ok=True, parents = True)
        save_dir_eval_ts.mkdir(exist_ok=True, parents = True)
        save_dir_meta_lic.mkdir(exist_ok=True, parents = True)
        save_dir_config.mkdir(exist_ok=True, parents = True)
        other_save_dirs = {'attr': save_dir_attr, 'eval_metr': save_dir_eval_metr, 'eval_ts' : save_dir_eval_ts,
                           'meta': save_dir_meta, 'meta_lic': save_dir_meta_lic, 'config': save_dir_meta}

    return save_dir_base, other_save_dirs


def _proc_check_input_df(df: pd.DataFrame, col_schema_df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks the input dataset for consistency in expected column format as generated from the yaml config file.

    :param df: The dataset of interest containing at a minimum catchment ID and evaluation metrics
    :type df: pd.DataFrame
    :param col_schema_df: The column schema naming convention ingested from the yaml file corresponding to the dataset.
    :type col_schema_df: pd.DataFrame
    :return: wide format df ensuring that the unique identifier for each row is 'gage_id'
    :rtype: pd.DataFrame

    note::
    Changelog/contributions
        2024-07-09, originally created, GL
        2024-07-11, bugfix in case index is already named 'gage_id', GL
    """


    gage_id = col_schema_df['gage_id'].iloc[0]
    metric_cols = col_schema_df['metric_cols'].iloc[0]
    metrics = metric_cols.split('|')
    if df.columns.str.contains(metric_cols).sum() != len(metrics):
        warnings.warn(f'Not all metric columns {', '.join(metrics)} are inside df columns. Revise the config file or ensure the input data is in appropriate format (i.e. wide format for each variable)')
 
    if not df.index.name == 'gage_id':
        # Change the name to gage_id   
        df.rename(columns = {gage_id : 'gage_id'},inplace=True)
        if not any(df.columns.str.contains('gage_id')):
            warnings.warn(f'Expecting one df column to be named: {gage_id} - per the config file. Inspect config file and/or dataframe col names')
        # Set gage_id as the index
        if any(df['gage_id'].duplicated()):
            warnings.warn('Expect only one gage_id for each row in the data. Convert df to wide format when passing to proc_col_schema(). This could create problems if writing standardized data in hierarchical format.')
        else: # We can set the index as 'gage_id'
            df.set_index('gage_id', inplace=True)
    return df

def proc_col_schema(df: pd.DataFrame, col_schema_df: pd.DataFrame, dir_save: str | os.PathLike) -> xr.Dataset:
    """
    Process model evaluation metrics into individual standardized files and save a standardized metadata file.

    :param df: pd.DataFrame type. The dataset of interest containing at a minimum catchment ID and evaluation metrics
    :type df: pd.DataFrame
    :param col_schema_df: The column schema naming convention ingested from the yaml file corresponding to the dataset. C
    :type col_schema_df: pd.DataFrame
    :param dir_save: Path for saving the standardized metric data file(s) and the metadata file.
    :type dir_save: str | os.PathLike
    :raises ValueError: _description_
    :return: _description_
    :rtype: xr.Dataset

    seealso:: :func:`read_schm_ls_of_dict`

    note:: 
    Changelog/contributions
        2024-07-02, originally created, GL
    """
    # Based on the standardized column schema naming conventions
    dataset_name =  col_schema_df['dataset_name'].iloc[0]
    formulation_id =  col_schema_df['formulation_id'].iloc[0]
    formulation_base =  col_schema_df['formulation_base'].iloc[0]
    save_type = col_schema_df['save_type'].iloc[0]
    save_loc = col_schema_df['save_loc'].iloc[0]
    if formulation_id == None:
        # Create formulation_id as a combination of formulation_base and other elements
        formulation_id = '_'.join(list(filter(None,[formulation_base, '_v',col_schema_df['formulation_ver'].iloc[0],'_',col_schema_df['dataset_name']]))) 
    
    # Create the unique filename corresponding to a dataset & formulation
    uniq_filename = f'{dataset_name}_{formulation_id}'

    # TODO add cloud or local saving
    if save_loc == 'local':
         # Optionally creates dir structure  if save_type == 'csv' or 'parquet'
        _save_dir_base, _other_save_dirs = _save_dir_struct(dir_save, dataset_name, save_type)
    elif save_loc == 'aws':
        print("TODO ensure connect credentials here")
        # TODO define _save_dir_base here in case .csv are desired in cloud

    # Run format checker/df renamer on input data based on config file's entries:
    df = _proc_check_input_df(df,col_schema_df)

    # Convert dataframe to the xarray dataset and add metadata:
    ds = df.to_xarray()
    ds.attrs = col_schema_df.to_dict('index')[0]
    
    # TODO query a database for the lat/lon corresponding to the gage-id if lat/lon not already provided

    # Save the standardized dataset
    if save_type == 'csv' or save_type == 'parquet':
        if len(_other_save_dirs) == 0:
            raise ValueError('Expected _save_dir_struct to generate values in _other_save_dirs')

        # TODO allow output write to a variety of locations (e.g. local/cloud)
        # Write data in long format
        save_path_eval_metr = Path(_other_save_dirs['eval_metr'] / f'{uniq_filename}.csv') 
        if save_type == 'csv':
            df.to_csv(save_path_eval_metr)
        else:
            df.to_parquet(Path(str(save_path_eval_metr).replace('.csv','.parquet')))
        # Write metadata table corresponding to these metric data table(s) (e.g. startDate, endDate)
        save_path_meta = Path(_other_save_dirs['meta'] / f'{uniq_filename}_metadata.csv')
        if save_type == 'csv':
            col_schema_df.to_csv(save_path_meta)
        else:
            col_schema_df.to_parquet(Path(str(save_path_meta).replace('.csv','.parquet')))

    elif save_type == 'netcdf':
        save_path_nc = Path(_save_dir_base/Path(f'{uniq_filename}.nc'))
        ds.to_netcdf(save_path_nc)
    elif save_type == 'zarr':
        save_path_zarr = Path(_save_dir_base/Path(f'{uniq_filename}.zarr'))
        ds.to_zarr(save_path_zarr)     

    return ds # Returning not intended use case, but it's an option