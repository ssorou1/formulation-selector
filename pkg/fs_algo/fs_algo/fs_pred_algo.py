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
 # TODO place in rafts_utils python package
def build_cfig_path(path_known_config:str | os.PathLike, path_or_name_cfig:str | os.PathLike) -> os.PathLike | None:
    """Build the expected configuration file path within the RAFTS framework

    :param path_known_config: path of the known configuration sent 
    :type path_known_config: str | os.PathLike
    :param path_or_name_cfig: Path or name of configuration file. If only name provided, it's assumed it resides in same directory as `path_known_config`
    :type path_or_name_cfig: str | os.PathLike
    :raises FileNotFoundError: The provided `path_known_config` does not exist
    :raises FileNotFoundError: The desired configuration file does not exist
    :return: The path to another relevant configuration file used for a different step in RAFTS processing
    :rtype: os.PathLike | None
    """
    dir_parent_cfig = Path(path_known_config).parent
    if not dir_parent_cfig.exists():
        raise FileNotFoundError(f"The provided 'known' configuration file does not exist: \n{path_known_config}")
    if path_or_name_cfig: # Only perform if path_or_name_cfig not None
        path_cfig = Path(dir_parent_cfig/Path(path_or_name_cfig))
        if not path_cfig.exists():
            path_cfig = Path(path_or_name_cfig)
            if not path_cfig.exists():
                raise FileNotFoundError(f'The following configuration file could not be found: \n{path_or_name_cfig}')
    else:
        warnings.warn("The configuration file may not have specified the path or file name.")
        path_cfig = None
    return path_cfig
# TODO create a function that's flexible/converts user formatted checks (a la fsds_proc)

def _read_pred_comid(path_pred_locs: str | os.PathLike, comid_pred_col:str ) -> np.typing.NDArray:
    if not Path(path_pred_locs).exists():
        FileNotFoundError(f"The path to prediction location data could not be found: \n{path_pred_locs} ")
    if '.csv' in Path(path_pred_locs).suffix:
        try:
            comids_pred = pd.read_csv(path_pred_locs)[comid_pred_col].values
        except:
            raise ValueError(f"Could not successfully read in {path_pred_locs} & select col {comid_pred_col}")
    else:
        raise ValueError(f"NEED TO ADD CAPABILITY THAT HANDLES {Path(path_pred_locs).suffix} file extensions")
    return comids_pred
# Predict values and evaluate predictions
if __name__ == "__main__":
    print("ALGORITHM PREDICTION")
    parser = argparse.ArgumentParser(description = 'process the prediction config file')
    parser.add_argument('path_pred_config', type=str, help='Path to the YAML configuration file specific for prediction.')
    # NOTE pred_config should contain the path for path_algo_config
    args = parser.parse_args()

    home_dir = Path.home()
    path_pred_config = Path(args.path_pred_config) #Path(f'{home_dir}/git/formulation-selector/scripts/eval_ingest/xssa/xssa_pred_config.yaml') 
    with open(path_pred_config, 'r') as file:
        pred_cfg = yaml.safe_load(file)

   
   
    path_algo_config = build_cfig_path(path_pred_config,pred_cfg.get('name_algo_config',None))
    path_attr_config = build_cfig_path(path_pred_config,pred_cfg.get('name_attr_config',None))

    with open(path_algo_config, 'r') as file:
        algo_cfg = yaml.safe_load(file)

    # algo_config = {k: algo_cfg['algorithms'][k][0] for k in algo_cfg['algorithms']}
    # if algo_config['mlp']['hidden_layer_sizes']: # purpose: evaluate string literal to a tuple
    #     algo_config['mlp']['hidden_layer_sizes'] = ast.literal_eval(algo_config['mlp']['hidden_layer_sizes'])

    #%%  Read the attribute configuration file corresponding to the algorithm training
    # try:
    #     name_attr_config = algo_cfg.get('name_attr_config', Path(path_algo_config).name.replace('algo','attr'))
    # except:
    #     print("TODO: allow attr_config reference to be specified in the prediction config, OR")
    #     print("OR specify the needed objects in the prediction config")
    # path_attr_config = Path(Path(path_algo_config).parent/name_attr_config)
    # if not Path(path_attr_config).exists():
    #     raise ValueError(f"Ensure that 'name_attr_config' as defined inside {path_algo_config.name} \
    #                       \n is also in the same directory as the algo config file {path_algo_config.parent}" )
    
    print("BEGINNING prediction of algorithm.")

    #%%  READ CONTENTS FROM THE ATTRIBUTE CONFIG
    attr_cfig = fsate.AttrConfigAndVars(path_attr_config)
    attr_cfig._read_attr_config()

    dir_base = attr_cfig.attrs_cfg_dict.get('dir_base')
    dir_std_base = attr_cfig.attrs_cfg_dict.get('dir_std_base')
    datasets = attr_cfig.attrs_cfg_dict.get('datasets') # Identify datasets of interest

    #%% ESTABLISH algorithim file i/o
    dir_out = fsate.fs_save_algo_dir_struct(dir_base).get('dir_out')
    dir_out_alg_base = fsate.fs_save_algo_dir_struct(dir_base).get('dir_out_alg_base')
    #%% PREDICTION FILE'S COMIDS
    path_pred_locs = pred_cfg.get('pred_file_in').format(**attr_cfig.attrs_cfg_dict)
    comid_pred_col = pred_cfg.get('pred_file_comid_colname')

    comids_pred = _read_pred_comid(path_pred_locs, comid_pred_col )

    #%% prediction config
    # TODO create pred config
    # path_pred_config = Path(args.path_pred_config)
    resp_vars = pred_cfg.get('algo_response_vars')
    algos = pred_cfg.get('algo_type')

    
    for ds in datasets:
        dir_out_alg_ds = Path(dir_out_alg_base/Path(ds))

        # TODO read algo evaluation metadata
        # df_meta = pd.read_parquet(dir_out_alg_base.str.replace('.joblib','.parquet'))
        # path_eval_dat =  str(fsate.std_algo_path(dir_out_alg_ds, algo='', metric='eval',
        #                                          dataset_id=ds)).replace("__eval__",'_eval_').replace('.joblib','.parquet')
        # pd.read_parquet(path_eval_dat)
        for metric in resp_vars:
            for algo in algos:
                path_algo = fsate.std_algo_path(dir_out_alg_ds, algo=algo, metric=metric, dataset_id=ds)
                if not Path(path_algo).exists():
                    raise FileNotFoundError(f"The following algorithm path does not exist: \n{path_algo}")


                # Read in the algorithm


                # Query the predictor variables for comids used for prediction


                # Perform prediction
               