'''
@title: Find CAMELS basins inside Julie Mai's xSSA dataset


'''
import pandas as pd
from pathlib import Path
import numpy as np
import yaml


# Define directories and paths
base_dir = '{home_dir}/noaa'.format(home_dir=str(Path.home()))
path_camels = Path(base_dir / Path('camels2/camels_hydro.txt'))
path_xssa = Path(base_dir / Path('regionalization/data/julemai-xSSA/data_in/basin_metadata/basin_validation_results.txt'))

save_dir = Path(Path(base_dir) / Path('regionalization/data/input/eval_metrics' ))
Path.mkdir(save_dir, exist_ok=True,parents = True)

config_path = '/Users/guylitt/Documents/notgit/xssa_schema.yaml' # TODO temp location

# Load the YAML configuration file
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

# ---- Read in Julie Mai's xSSA results
df_xssa_val = pd.read_csv(path_xssa,sep = '; ',dtype={'basin_id' :str})

# Ensure appropriate str formats & remove extraneous spaces
df_xssa_val.columns = df_xssa_val.columns.str.replace(' ','')
df_xssa_val['basin_id'] = df_xssa_val['basin_id'].str.replace(' ','')

# ----- Read in CAMELS data (simply to retrieve the gauge_ids)
df_camlh = pd.read_csv(path_camels,sep=';',dtype={'gauge_id' :str})

# Ensure appropriate str formats & remove extraneous spaces
df_camlh['gauge_id'] = df_camlh['gauge_id'].str.replace(' ','')

# ----- Subset the xssa dataset to CAMELS basins
df_camls_xssa_val = df_camlh.merge(df_xssa_val, left_on= 'gauge_id', right_on = 'basin_id', how='inner')
df_sub = df_camls_xssa_val.drop(columns = df_camlh.columns)

# ----- Change to standard structure: # TODO convert this to a naming schema 
col_schema_df = pd.DataFrame({
              # required minimum
              'GageID' : 'basin_id',
              'metricCols' : 'nse|rmse|kge',
              # high priority metadata
              'dataset_name' : 'juliemai-xSSA',
              'FormulationBase': 'Raven_blended',
              'FormulationID' : 'Raven_blended',
              'TemporalRes' : 'daily',
              'TargetVar' : 'Q',
              'StartDate' : '',
              'EndDate' : '',
              'CalYN' : 'Y',
              # additional metadata
              'RR' : '',
              'SP' : '',
               'GW' : '',
              'input_filepath' : '{base_dir}/julemai-xSSA/data_in/basin_metadata/basin_validation_results.txt',
              'source_url': 'https://zenodo.org/records/5730428',
              'dataset_doi' : '10.5281/zenodo.5730428',
              'literature_doi' : 'https://doi.org/10.1038/s41467-022-28010-7'
            },index=[0])


def proc_col_schema(df, col_schema_df):

    metricCols = col_schema_df['metricCols'].iloc[0]
    gageID = col_schema_df['GageID'].iloc[0]
    dataset_name =  col_schema_df['dataset_name'].iloc[0]
    FormulationBase =  col_schema_df['FormulationBase'].iloc[0]
    FormulationID =  col_schema_df['FormulationID'].iloc[0]
    TemporalRes =  col_schema_df['TemporalRes'].iloc[0]
    TargetVar =  col_schema_df['TargetVar'].iloc[0]
    StartDate =  col_schema_df['StartDate'].iloc[0]
    EndDate =  col_schema_df['EndDate'].iloc[0]
    CalYN =  col_schema_df['CalYN'].iloc[0]
    rr =  col_schema_df['RR'].iloc[0]
    sp = col_schema_df['SP'].iloc[0]
    gw =  col_schema_df['GW'].iloc[0]
    dataset_doi =  col_schema_df['dataset_doi'].iloc[0]
    literature_doi =  col_schema_df['literature_doi'].iloc[0]

    metrics = metricCols.split('|')

    # TODO configure which id_vars are expected in raw dataset
    df_melt_metr = pd.melt(df, id_vars = [gageID], value_vars =metrics, var_name = 'Metric', value_name = 'MetricValue')
    # Rename gageID
    df_melt_metr.rename(columns = {gageID : 'GageID'},inplace=True)

    # Iterate over each metric
    for metr, data_metr in df_melt_metr.groupby('Metric'):
        print(f'Saving {metr} with total CAMELS basins of {df_sub_xssa_val.shape[0]}')
        save_path = Path(save_dir / f'{dataset_name}_{FormulationID}_{metr}.csv') # TODO is FormulationID desired in the filename
        data_metr.to_csv(save_path)

proc_col_schema(df_sub, col_schema_df)