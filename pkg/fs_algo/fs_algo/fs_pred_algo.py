import argparse
import yaml
import joblib
import fs_algo.fs_algo_train_eval as fsate
import pandas as pd
from pathlib import Path
import ast
import warnings
import os
import numpy as np
import forestci as fci
from sklearn.model_selection import train_test_split

# TODO create a function that's flexible/converts user formatted checks (a la fs_proc)


# Predict values and evaluate predictions
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'process the prediction config file')
    # parser.add_argument('path_pred_config', type=str, help='Path to the YAML configuration file specific for prediction.')
    parser.add_argument('path_pred_config', type=str, nargs='?', default='C:/Users/Soroush.Sorourian/git/formulation-selector/scripts/eval_ingest/xssa/xssa_pred_config.yaml', help='Path to the YAML configuration file specific for prediction.')
    # NOTE pred_config should contain the path for path_algo_config
    args = parser.parse_args()

    home_dir = Path.home()
    # path_pred_config = Path(args.path_pred_config) #Path(f'{home_dir}/git/formulation-selector/scripts/eval_ingest/xssa/xssa_pred_config.yaml') 
    path_pred_config = Path(f'C:/Users/Soroush.Sorourian/git/formulation-selector/scripts/eval_ingest/xssa/xssa_pred_config.yaml') 
    with open(path_pred_config, 'r') as file:
        pred_cfg = yaml.safe_load(file)

    #%%  READ CONTENTS FROM THE ATTRIBUTE CONFIG
    path_attr_config = fsate.build_cfig_path(path_pred_config,pred_cfg.get('name_attr_config',None))
    attr_cfig = fsate.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()

    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')
    dir_db_attrs = attr_cfig.attrs_cfg_dict.get('dir_db_attrs')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets') # Identify datasets of interest
    attrs_sel = attr_cfig.attrs_cfg_dict.get('attrs_sel', None)

    #%% ESTABLISH ALGORITHM FILE I/O
    dir_out = fsate.fs_save_algo_dir_struct(dir_base).get('dir_out')
    dir_out_alg_base = fsate.fs_save_algo_dir_struct(dir_base).get('dir_out_alg_base')
    #%% PREDICTION FILE'S COMIDS
    path_pred_locs = pred_cfg.get('pred_file_in').format(**attr_cfig.attrs_cfg_dict)
    comid_pred_col = pred_cfg.get('pred_file_comid_colname')

    comids_pred = fsate._read_pred_comid(path_pred_locs, comid_pred_col )

    #%% prediction config
    # TODO create pred config
    # path_pred_config = Path(args.path_pred_config)
    resp_vars = pred_cfg.get('algo_response_vars')
    algos = pred_cfg.get('algo_type')


    #%%  Read in predictor variable data (aka basin attributes) 
    # Read the predictor variable data (basin attributes) generated by proc.attr.hydfab
    df_attr = fsate.fs_read_attr_comid(dir_db_attrs, comids_pred, attrs_sel = attrs_sel,
                                    _s3 = None,storage_options=None)
    # Convert into wide format for model training
    df_attr_wide = df_attr.pivot(index='featureID', columns = 'attribute', values = 'value')
    #%% Run prediction
    for ds in datasets:
        dir_out_alg_ds = Path(dir_out_alg_base/Path(ds))
        print(f"PREDICTING algorithm for {ds}")
        for metric in resp_vars:
            for algo in algos:
                path_algo = fsate.std_algo_path(dir_out_alg_ds, algo=algo, metric=metric, dataset_id=ds)
                if not Path(path_algo).exists():
                    raise FileNotFoundError(f"The following algorithm path does not exist: \n{path_algo}")


                pipeline_with_ci = joblib.load(path_algo)
                # pipe = pipeline_with_ci.get('pipe', None)
                
                pipe = pipeline_with_ci['pipe']  # Assign the actual pipeline (pipe) to 'pipe'
                rf_model = pipe.named_steps['randomforestregressor']  # Use the correct step name
                feat_names = list(pipe.feature_names_in_)
                df_attr_sub = df_attr_wide[feat_names]

                # Perform prediction
                resp_pred = pipe.predict(df_attr_sub)
                
                # Calculate confidence intervals for the predictions using forestci
                # X_train = joblib.load(Path(dir_out_alg_ds, 'X_train.joblib'))  # Load saved training data
                X_train = pd.read_csv(Path(dir_out_alg_ds) / 'X_train.csv')                    
                pred_ci = fci.random_forest_error(forest=rf_model, X_train_shape=X_train.shape, X_test=df_attr_sub.to_numpy())

                # compile prediction results:
                df_pred =pd.DataFrame({'comid':comids_pred,
                             'prediction':resp_pred,
                             'ci': pred_ci,
                             'metric':metric,
                             'dataset':ds,
                             'algo':algo,
                             'name_algo':Path(path_algo).name})
                
                path_pred_out = fsate.std_pred_path(dir_out,algo=algo,metric=metric,dataset_id=ds)
                # Write prediction results
                df_pred.to_parquet(path_pred_out)
                print(f"   Completed {algo} prediction of {metric}")
