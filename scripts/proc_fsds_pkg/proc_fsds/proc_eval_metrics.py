'''
@title: Helper functions for processing evaluation metrics datasets
@author: Guy Litt <guy.litt@noaa.gov>
@description: functions read in yaml schema and standardize metrics datasets

Changelog/contributions
    2024-07-02 Originally created, GL
'''


import pandas as pd
from pathlib import Path
import yaml

def read_schm_ls_of_dict(schema_path):
    '''
    @title: Read a dataset's schema file designed as a list of dicts
    @param: schema_path the filepath to the schema

    Changelog/contributions
        2024-07-02 Originally created, GL
    '''
    # Load the YAML configuration file
    with open(schema_path, 'r') as file:
        config = yaml.safe_load(file)

    # Convert dict of lists into pd.DataFrame
    ls_form = list()
    for k, vv in config.items():
        for v in vv:
            ls_form.append(pd.DataFrame(v, index = [0]))
    df_all = pd.concat(ls_form, axis=1)

    # TODO add checks on expected schema format, names, required variables, etc.

    return df_all


def proc_col_schema(df, col_schema_df, dir_save):
    '''
    @title: Process model evaluation metrics into individual standardized files and save a standardized metadata file.
    @author: Guy Litt <guy.litt@noaa.gov>
    @description: Creates an individual data file for each metric, and saves a metadata file in the dir_save.
    The standard format is the following columns: 'gage_id', 'metric', 'metric_val'
    @param: df pd.DataFrame type. The dataset of interest containing at a minimum catchment ID and evaluation metrics
    @param: col_schema_df pd.DataFrame type. The column schema naming convention ingested from the yaml file corresponding to the dataset.
    @param: dir_save str or pathlib.Path type. Path for saving the standardized metric data file(s) and the metadata file.

    Changelog/contributions
        2024-07-02, originally created, GL
    '''
    # Based on the standardized column schema naming conventions
    metric_cols = col_schema_df['metric_cols'].iloc[0]
    gage_id = col_schema_df['gage_id'].iloc[0]
    dataset_name =  col_schema_df['dataset_name'].iloc[0]
    formulation_id =  col_schema_df['formulation_id'].iloc[0]
    formulation_base =  col_schema_df['formulation_base'].iloc[0]
    if formulation_id == None:
        # Create formulation_id as a combination of formulation_base and other elements
        formulation_id = '_'.join(list(filter(None,[formulation_base, col_schema_df['rr'].iloc[0], col_schema_df['sp'].iloc[0],col_schema_df['gw'].iloc[0]])))

    # TODO consider whether any of these other variables are of interest
    # temporal_res =  col_schema_df['temporal_res'].iloc[0]
    # target_var =  col_schema_df['target_var'].iloc[0]
    # start_date =  col_schema_df['start_date'].iloc[0]
    # end_date =  col_schema_df['end_date'].iloc[0]
    # cal_yn =  col_schema_df['cal_yn'].iloc[0]
    # rr =  col_schema_df['rr'].iloc[0]
    # sp = col_schema_df['sp'].iloc[0]
    # gw =  col_schema_df['gw'].iloc[0]
    # dataset_doi =  col_schema_df['dataset_doi'].iloc[0]
    # literature_doi =  col_schema_df['literature_doi'].iloc[0]
    
    # TODO configure which id_vars are expected in raw dataset
    # Convert dataset into the standarzied format, with the var_name = 'metric' and the value_name = 'metric_val'
    metrics = metric_cols.split('|')
    df_melt_metr = pd.melt(df, id_vars = gage_id, value_vars =metrics, var_name = 'metric', value_name = 'metric_val')
    # Rename the gage_id in the original dataset (e.g. 'basin_id') to the standard 'gage_id'
    df_melt_metr.rename(columns = {gage_id : 'gage_id'},inplace=True)

    # TODO create a function that optionally writes locally, to cloud, database, etc.
    Path(dir_save).mkdir(exist_ok=True, parents = True)
    
    # Iterate over each metric & write each one to file individually
    for metr, data_metr in df_melt_metr.groupby('metric'):
        print(f'Saving {metr} with total basins n={data_metr.shape[0]}')
        # TODO is FormulationID desired in the standardized filename?
        save_path = Path(Path(dir_save) / f'{dataset_name}_{formulation_id}__{metr}.csv') 
        data_metr.to_csv(save_path)
    # Define the standard save_path structure
    col_schema_df['save_path_format'] = str(save_path).replace(metr,'_')
    # Write metadata table corresponding to these metric data table(s) (e.g. startDate, endDate)
    save_path_meta = Path(Path(dir_save) / f'{dataset_name}_{formulation_id}_metadata.csv')
    
    col_schema_df.to_csv(save_path_meta)
 
    return df_melt_metr # Returning not intended use case, but it's an option
