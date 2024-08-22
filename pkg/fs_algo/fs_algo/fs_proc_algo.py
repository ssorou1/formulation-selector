
from sklearn.model_selection import train_test_split
import yaml
import pandas as pd
from pathlib import Path
import xarray as xr


# Read in yaml file
path_config =  '/Users/guylitt/git/fsds/scripts/eval_ingest/xssa/xssa_attr_config.yaml' 


# attribute data of interest:
# Attribute data location:
with open(path_config, 'r') as file:
    config = yaml.safe_load(file)

# TODO create a model configuration file
algo_config = {'rf': {'n_estimators':100},
                    'mlp': {'hidden_layer_sizes' :(4,),
                             'activation':'relu',
                             'solver':'lbfgs',
                             'alpha':0.001,
                             'batch_size':'auto',
                             'learning_rate':'constant',
                             'power_t':0.5,
                             'max_iter':10000,
                             }}


home_dir = str(Path.home())
dir_base = list([x for x in config['file_io'] if 'dir_base' in x][0].values())[0].format(home_dir=home_dir)
# Location of attributes (predictor data):
dir_db_attrs = list([x for x in config['file_io'] if 'dir_db_attrs' in x][0].values())[0].format(dir_base = dir_base)

# parent location of response variable data:
dir_std_base =  list([x for x in config['file_io'] if 'dir_std_base' in x][0].values())[0].format(dir_base = dir_base)

# Identify datasets of interest:
datasets = list([x for x in config['formulation_metadata'] if 'datasets' in x][0].values())[0]

for ds in datasets: 
    # TODO implement a check to ensure each dataset directory exists
    path_nc = [x for x in Path(dir_std_base/Path(ds)).glob("*.nc") if x.is_file()]
    #path_zarr = [x for x in Path(dir_std_base/Path(ds)).glob("*.zarr") if x.is_dir()]
    try:
        dat_resp = xr.open_dataset(path_nc, engine='netcdf4')
    except:
        path_zarr = [x for x in Path(dir_std_base/Path(ds)).glob("*.zarr")]
        try:
            dat_resp = xr.open_dataset(path_zarr,engine='zarr')
        except:
            raise ValueError(f"Could not identify an approach to read in dataset via {path_nc} nor {path_zarr}")
    # The metrics approach

    metrics = dat_resp.attrs['metric_mappings'].split('|')
    gage_ids = dat_resp['gage_id'].values

    # %% COMID retrieval and assignment to response variable's coordinate
    [featureSource,featureID] = _find_feat_srce_id(dat_resp,config)
    comids_resp = fs_retr_nhdp_comids(featureSource,featureID,gage_ids)
    dat_resp = dat_resp.assign_coords(comid = comids_resp)
    # TODO allow secondary option where featureSource and featureIDs already provided, not COMID 

    #%%  Read in predictor variable data (aka basin attributes) 
    # TODO list of variables of interest:
    # TODO  Setup the s3fs filesystem that is going to be used by xarray to open the parquet files
    _s3 = s3fs.S3FileSystem(anon=True)

    # TODO subset based on variables of interest
    # attr_arr = attr_df_sub.to_dask_array(lengths=True)
    dd_attr = fs_read_attr_comid(dir_db_attrs, comids_resp, attrs_sel = 'all',_s3 = None,storage_options=None)

    # NOTE: additional subsetting may be performed on attr_df here before computing 
    df_attr = dd_attr.compute() # Create a pandas DataFrame
    vars = df_attr['attribute'].unique()
    df_attr_wide = df_attr.pivot(index='featureID', columns = 'attribute', values = 'value')
    dd_resp = dat_resp.to_dask_dataframe()
    

    #%% Join attribute data and response data
    for metr in metrics:
        # Subset response data to metric of interest & the comid
        df_metr_resp = pd.DataFrame({'comid': dat_resp['comid'],
                                     metr : dat_resp[metr].data})
        
        df_pred_resp = df_metr_resp.merge(df_attr_wide, left_on = 'comid', right_on = 'featureID')

        # %% TRAIN ALGORITHMS AND EVALUATE PERFORMANCE
        # Train/test split
        X = df_pred_resp[vars]
        y = df_pred_resp[metr]
        X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.3, random_state=32)

        alg_cfig = algo_config.copy() # Back up the original model config before popping

        # Initialize the trainer with algo_config and metric
        trainer = AlgoTrainEval(algo_config={'rf': {'n_estimators': 100}, 
                                                'mlp': {'hidden_layer_sizes': (100,), 'max_iter': 300}},
                                metr='accuracy')

        # Train algorithms
        algs_dict = trainer.train_algos(X_train, y_train)

        # Make predictions
        preds_dict = trainer.predict_algos(X_test)

        # Evaluate predictions
        eval_dict = trainer.evaluate_algos(y_test, preds_dict)
dat_resp.close()
