'''
Unit testing for AlgoTrainEval class in the fs_algo package

example
> coverage run -m unittest test_algo_train_eval.py  
> coverage report
> coverage html 
'''
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import tempfile
import joblib
import os
from pathlib import Path
from fs_algo.fs_algo_train_eval import AlgoTrainEval  

# %% UNIT TESTING FOR NHDplus/dataset munging


# %% UNIT TEST FOR AlgoTrainEval class
class TestAlgoTrainEval(unittest.TestCase):

    def setUp(self):
        # Create a simple DataFrame for testing
        data = {
            'attr1': [1, 2, 3, 4, 5],
            'attr2': [5, 4, 3, 2, 1],
            'metric1': [0.1, 0.9, 0.3, 0.1, 0.8]
        }
        self.df = pd.DataFrame(data)

        # Variables and configurations for algorithms
        self.vars = ['attr1', 'attr2']
        self.algo_config = {
            'rf': {'n_estimators': 10},
            'mlp': {'hidden_layer_sizes': (10,), 'max_iter': 200}
        }
        self.dataset_id = 'test_dataset'
        self.metric = 'metric1'

        # Output directory
        self.dir_out_alg_ds = tempfile.gettempdir()

        # Instantiate AlgoTrainEval class
        self.train_eval = AlgoTrainEval(df=self.df, vars=self.vars, algo_config=self.algo_config,
                                 dir_out_alg_ds=self.dir_out_alg_ds, dataset_id=self.dataset_id,
                                 metr=self.metric, test_size=0.4, rs=42)

    def test_split_data(self):
        # Test data splitting
        self.train_eval.split_data()
        self.assertEqual(len(self.train_eval.X_train), 3)
        self.assertEqual(len(self.train_eval.X_test), 2)
        self.assertEqual(len(self.train_eval.y_train), 3)
        self.assertEqual(len(self.train_eval.y_test), 2)

    def test_train_algos(self):
        # Test algorithm training
        self.train_eval.split_data()
        self.train_eval.train_algos()

        self.assertIn('rf', self.train_eval.algs_dict)
        self.assertIsInstance(self.train_eval.algs_dict['rf']['algo'], RandomForestRegressor)

        self.assertIn('mlp', self.train_eval.algs_dict)
        self.assertIsInstance(self.train_eval.algs_dict['mlp']['algo'], MLPRegressor)

        self.assertEqual(len(self.algo_config), len(self.train_eval))

    def test_predict_algos(self):
        # Test algorithm predictions
        self.train_eval.split_data()
        self.train_eval.train_algos()

        preds = self.train_eval.predict_algos()

        self.assertIn('rf', preds)
        self.assertIn('mlp', preds)
        self.assertEqual(len(preds['rf']['y_pred']), 2)  # Number of test samples
        self.assertEqual(len(preds['mlp']['y_pred']), 2)

    def test_evaluate_algos(self):
        # Test evaluation of algorithms
        self.train_eval.split_data()
        self.train_eval.train_algos()
        self.train_eval.predict_algos()

        eval_dict = self.train_eval.evaluate_algos()

        self.assertIn('rf', eval_dict)
        self.assertIn('mlp', eval_dict)
        self.assertIn('mse', eval_dict['rf'])
        self.assertIn('r2', eval_dict['mlp'])

    @patch('joblib.dump')
    def test_save_algos(self, mock_dump):
        # Test saving algorithms to disk
        self.train_eval.split_data()
        self.train_eval.train_algos()

        # Mock joblib.dump to avoid file operations
        self.train_eval.save_algos()
        self.assertTrue(mock_dump.called)

        for algo in self.train_eval.algs_dict.keys():
            self.assertIn('loc_algo', self.train_eval.algs_dict[algo])

    def test_org_metadata_alg(self):
        # Test organizing metadata
        self.train_eval.split_data()
        self.train_eval.train_algos()
        self.train_eval.predict_algos()
        self.train_eval.evaluate_algos()

        # Mock saving algorithms and call organization
        with patch('joblib.dump'):
            self.train_eval.save_algos()

        self.train_eval.org_metadata_alg()

        # Check eval_df is correctly populated
        self.assertFalse(self.train_eval.eval_df.empty)
        self.assertIn('dataset', self.train_eval.eval_df.columns)
        self.assertIn('loc_algo', self.train_eval.eval_df.columns)
        self.assertIn('algo', self.train_eval.eval_df.columns)
        self.assertEqual(self.train_eval.eval_df['dataset'].iloc[0], self.dataset_id)

    def test_train_eval(self):
        # Test the overall wrapper method
        with patch('joblib.dump'):
            self.train_eval.train_eval()

        # Check all steps completed successfully
        self.assertFalse(self.train_eval.eval_df.empty)
        self.assertIn('rf', self.train_eval.algs_dict)
        self.assertIn('mlp', self.train_eval.algs_dict)
        self.assertIn('rf', self.train_eval.preds_dict)
        self.assertIn('mlp', self.train_eval.preds_dict)
        self.assertIn('rf', self.train_eval.eval_dict)
        self.assertIn('mlp', self.train_eval.eval_dict)


if __name__ == '__main__':
    unittest.main()