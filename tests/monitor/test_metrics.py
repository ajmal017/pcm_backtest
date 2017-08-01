import numpy as np
import numpy.testing as npt
import pandas as pd
import pandas_datareader.data as web

from unittest import TestCase
from datetime import datetime

from pcm_backtest.metrics import (
	sharpe_ratio, var_cov_var, drawdown,
	water_mark, max_drawdown_dur
)


class TestSharpe(TestCase):
	def setUp(self):
		np.random.seed(0)
		self.rets = np.e ** (np.random.lognormal(0.00065,0.016,1000) - 1) - 1
		self.rfr = 0.03
		self.N = 250


	def test_sharpe_ratio(self):
		excess_dailly_ret = self.rets - self.rfr / self.N

		self.assertAlmostEqual(
			sharpe_ratio(excess_dailly_ret, self.N),
			0.055451455606353478
		)


class TestVaR(TestCase):
	def setUp(self):
		self.P = 1e6
		self.c = 0.95

		np.random.seed(0)
		self.rets = np.e ** (np.random.lognormal(0.00065,0.016,1000) - 1) - 1


	def test_var(self):
		mu = np.mean(self.rets)
		sigma = np.std(self.rets)

		self.assertAlmostEqual(
			var_cov_var(self.P, self.c, mu, sigma),
			25833.226070465054
		)


class TestDrawdown(TestCase):
	def setUp(self):
		self.pnl = pd.Series([10,15,13,9,30,31,35,31,25])


	def test_drawdown(self):
		npt.assert_array_almost_equal(
			drawdown(self.pnl),
			np.array([
				np.nan,  0., 0.133333, 0.4, 0., 0.,
        		0., 0.114286, 0.285714
        	])
		)


class TestWaterMark(TestCase):
	def setUp(self):
		self.pnl = pd.Series([10,15,13,9,30,31,35,31,25])
		self.pnl2 = pd.Series([30,29,28,35,36,37,39,20,19])


	def test_hwm(self):
		npt.assert_array_almost_equal(
			water_mark(self.pnl, how='high'),
			np.array([10,15,15,15,30,31,35,35,35])
		)


	def test_lwm(self):
		npt.assert_array_almost_equal(
			water_mark(self.pnl2, how='low'),
			np.array([30,29,28,28,28,28,28,20,19])
		)


class TestDrawdownDuration(TestCase):
	def setUp(self):
		self.pnl = pd.Series([10,15,13,12,9,30,31,35,31,25,26,34.5])


	def test_drawdown_max_dur(self):
		self.assertEqual(max_drawdown_dur(self.pnl), 4)
