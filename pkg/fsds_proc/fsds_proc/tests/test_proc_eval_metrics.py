'''
Unit tests for the fsds_proc package
@author: Guy Litt <guy.litt@noaa.gov>


Changelog/contributions
    2024-07-10 Originally created, GL
'''


import unittest
from pathlib import Path
import pandas as pd
import yaml
import tempfile
import xarray as xr
from fsds_proc.proc_eval_metrics import read_schm_ls_of_dict, proc_col_schema, _proc_check_input_config, _proc_flatten_ls_of_dict_keys


# TODO move setup.py to ../setup.py and call > python -m unittest tests.test_proc_eval_metrics

parent_dir_test = Path(__file__).parent
print(parent_dir_test)

schema_dir_test = Path(parent_dir_test/Path("user_data_schema.yaml"))

# Load the user-specific metrics dataset
test_dat_user = pd.read_csv(Path(parent_dir_test/Path("user_metric_data.csv")))

# Load the YAML configuration file
with open(schema_dir_test, 'r') as file:
    config = yaml.safe_load(file)

# Define the unit test saving directory as a temp dir
dir_save = tempfile.gettempdir()

config_df = read_schm_ls_of_dict(schema_dir_test)

class TestReadSchmLsOfDict(unittest.TestCase):
    '''
    A normal run
    '''
    def test_identical(self):
        global parent_dir_test
        global schema_dir_test
        exp_config_df = pd.read_csv(Path(parent_dir_test/Path("test_config_df.csv")), index_col=False)
        gen_config_df = read_schm_ls_of_dict(schema_dir_test)
        pd.testing.assert_frame_equal(exp_config_df, gen_config_df, check_dtype = False)


class TestProcColSchema(unittest.TestCase):
    '''
    A normal run
    '''
    global config_df
    global dir_save
    exp_config_df = pd.read_csv(Path(parent_dir_test/Path("test_config_df.csv")), index_col=False)
    dds = proc_col_schema(exp_config_df, config_df, dir_save)

    def test_dataset_type(self):
        self.assertIsInstance(self.dds, xr.Dataset)

class TestProcCheckInputConfig(unittest.TestCase):
    
    def test_std_keys(self):
        global config
        with self.assertRaises(Exception) as context:
            _proc_check_input_config(config, std_keys = ['not the standard keys'])
            self.assertTrue(' provided keys in the input config file' in str(context.exception))
    def test_std_col(self):
        global config
        self.assertRaises(ValueError, 
                          _proc_check_input_config, config, req_col_schema = ['not the standard col names'])
    def test_form_meta(self):
        global config
        self.assertRaises(ValueError, 
                          _proc_check_input_config, config, req_form_meta = ['not the standard formulation metadata'])
    def test_file_io(self):
        global config
        self.assertRaises(ValueError, 
                          _proc_check_input_config, config, req_file_io = ['not the standard dir or save keys'])



class TestProcFlattenLsOfDictKeys(unittest.TestCase):
    global config
    ls_fio = _proc_flatten_ls_of_dict_keys(config, 'file_io')

    def test_return_ls(self):
        self.assertTrue(type(self.ls_fio) == list)

    def test_size_ls(self):
        self.assertTrue(len(self.ls_fio) == 5)


if __name__ == '__main__':
    unittest.main()
