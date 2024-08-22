
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd
from pathlib import Path
import xarray as xr
import pynhd as nhd
import dask_expr
import dask.dataframe as dd
import os
from collections.abc import Iterable
from typing import List, Optional, Dict

# %% ATTRIBUTES
def fs_read_attr_comid(dir_db_attrs:str | os.PathLike, comids_resp:list, attrs_sel = 'all',
                       _s3 = None,storage_options=None)-> dask_expr._collection.DataFrame:
    if _s3:
        storage_options={"anon",True} # for public
    
    # Read attribute data acquired using fsds.attr.hydfab R package
    all_attr_df = dd.read_parquet(dir_db_attrs, storage_options = storage_options)

    # Subset based on comids of interest
    attr_df_subloc = all_attr_df[all_attr_df['featureID'].str.contains('|'.join(comids_resp))]

    if attrs_sel == 'all':
        # TODO shold figure out which attributes are common across all data when using 'all'
        attrs_sel = attr_df_subloc['attribute'].unique().compute()

    attr_df_sub = attr_df_subloc[attr_df_subloc['attribute'].str.contains('|'.join(attrs_sel))]
    return attr_df_sub

def _find_feat_srce_id(dat_resp: Optional[xr.core.dataset.Dataset] = None,
                       config: Optional[Dict] = None) -> List[str]:
    # Attempt to grab dataset attributes (in cases where it differs by dataset), fallback on config file
    featureSource = None
    try: # dataset attributes first
        featureSource = dat_resp.attrs.get('featureSource', None)
    except (KeyError, StopIteration): # config file second
        featureSource = next(x['featureSource'] for x in config['col_schema'] if 'featureSource' in x)
    if not featureSource:
        raise ValueError(f'The featureSource could not be found. Ensure it is present in the col_schema section {path_config}')
    # Attempt to grab featureID from dataset attributes, fallback to the config file
    featureID = None
    try: # dataset attributes first
        featureID = dat_resp.attrs.get('featureID', None)
    except (KeyError, StopIteration): # config file second
        featureID = next(x['featureID'] for x in config['col_schema'] if 'featureID' in x)
    if not featureID:
        raise ValueError(f'The featureID could not be found. Ensure it is present in the col_schema section of {path_config}')
        # TODO need to map gage_id to location identifier in attribute data!

    return [featureSource, featureID]

def fs_retr_nhdp_comids(featureSource:str,featureID:str,gage_ids: Iterable[str] ) ->list:
    # Retrieve response variable's comids, querying the shortest distance in the flowline
    nldi = nhd.NLDI()
    comids_resp = [nldi.navigate_byid(fsource=featureSource,fid= featureID.format(gage_id=gage_id),
                                navigation='upstreamMain',
                                source='flowlines',
                                distance=1).loc[0]['nhdplus_comid'] 
                                for gage_id in gage_ids.values]
    return comids_resp

# %% ALGORITHM TRAINING AND EVALUATION
class AlgoTrainEval:
    def __init__(self, algo_config: dict, metr: str = None):
        self.algo_config = algo_config
        self.metric = metr
        self.algs_dict = {}

    def train_algos(self, X_train: pd.DataFrame, y_train: pd.Series):
        # Train algorithms based on config
        if 'rf' in self.algo_config:  # RANDOM FOREST
            rf = RandomForestRegressor(n_estimators=self.algo_config['rf'].pop('n_estimators'),
                                       random_state=32)
            rf.fit(X_train, y_train)
            self.algs_dict['rf'] = {'algo': rf,
                                    'type': 'random forest regressor',
                                    'metric': self.metric}

        if 'mlp' in self.algo_config:  # MULTI-LAYER PERCEPTRON
            mlpcfg = self.algo_config['mlp']
            mlp = MLPRegressor(random_state=32,
                               hidden_layer_sizes=mlpcfg.pop('hidden_layer_sizes', (100,)),
                               activation=mlpcfg.pop('activation', 'relu'),
                               solver=mlpcfg.pop('solver', 'lbfgs'),
                               alpha=mlpcfg.pop('alpha', 0.001),
                               batch_size=mlpcfg.pop('batch_size', 'auto'),
                               learning_rate=mlpcfg.pop('learning_rate', 'constant'),
                               power_t=mlpcfg.pop('power_t', 0.5),
                               max_iter=mlpcfg.pop('max_iter', 200))
            mlp.fit(X_train, y_train)
            self.algs_dict['mlp'] = {'algo': mlp,
                                     'type': 'multi-layer perceptron regressor',
                                     'metric': self.metric}

        return self.algs_dict

    def predict_algos(self, X_test: pd.DataFrame):
        # Make predictions with trained algorithms
        preds_dict = {}
        for k, v in self.algs_dict.items():
            algo = v['algo']
            y_pred = algo.predict(X_test)
            preds_dict[k] = {'y_pred': y_pred,
                             'type': v['type'],
                             'metric': v['metric']}
        return preds_dict

    def evaluate_algos(self, y_test: pd.Series, preds_dict: dict):
        # Evaluate the predictions
        eval_dict = {}
        for k, v in preds_dict.items():
            y_pred = v['y_pred']
            eval_dict[k] = {'type': v['type'],
                            'metric': v['metric'],
                            'mse': mean_squared_error(y_test, y_pred),
                            'r2': r2_score(y_test, y_pred)}
        return eval_dict


