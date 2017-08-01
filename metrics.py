import numpy as np, pandas as pd
import pandas_datareader.data as web

from datetime import datetime
from scipy.stats import norm
from .conf import Constant


def sharpe_ratio(returns, N=250):
	"""
	Arugments
	---------
	returns: Series | Numpy Array
	N: number of period to measure
		Daily (252), Hourly (252*6.5),  Minutely(252*6.5*60) etc.
	"""
	er = np.mean(returns)
	rf = (Constant.Rf + 1) ** (1/N) - 1
	return np.sqrt(N) * (er - rf) / np.std(returns, ddof=1)


def roy_safety_ratio(returns, target_returns, N=252):
	"""
	Arugments
	---------
	returns: Series | Numpy Array
	target_returns: Series | Numpy Array
	N: number of period to measure
		Daily (252), Hourly (252*6.5),  Minutely(252*6.5*60) etc.
	"""
	return np.sqrt(N) * (np.mean(returns) - np.mean(target_returns)) / np.std(returns)


def alpha_beta(returns, target_returns, N=252):
	"""
	Arugments
	---------
	returns: Series | Numpy Array
	target_returns: Series | Numpy Array
	N: number of period to measure
		Daily (252), Hourly (252*6.5),  Minutely(252*6.5*60) etc.


	Return
	------
	alpha, beta
	"""
	X = np.asarray(target_returns).reshape(-1, 1)
	X = np.concatenate([np.ones(X.shape), X], axis=1)

	y = np.asarray(returns).reshape(-1, 1)

	alpha, beta = (np.linalg.pinv(X.T @ X) @ X.T @ y).flatten()
	alpha = (alpha+1)**N-1
	return alpha, beta


def var_cov_var(P, c, mu, sigma):
	"""
	Value at Risk - Variance Covariance Method

	Arugments
	---------
	P: portfolio dollars
	c: confidence level, the tail percentage
	mu: mean return
	sigma: standard deviation of return
	"""
	alpha = norm.ppf(1-c, mu, sigma)
	return P - P * (alpha + 1)


def water_mark(pnl, how='high'):
	"""Accumulative Maximum/Minimum of a Series

	Parameter
	---------
	pnl: Pandas Series
		- index of timestamp
		- period percentagized returns

	Return
	------
	Series
	"""
	mark = np.maximum if how == 'high' else np.minimum
	return mark.accumulate(pnl.fillna(pnl.min()))


def drawdown(pnl):
	"""
	- Calcualte the largest peak-to-through drawdown of the PnL Curve
	- As well as the Duration of the drawdown

	Parameter
	---------
	pnl: Pandas Series 
		- index of timestamp
		- period percentagized returns

	Return
	------
	drawdown
	"""
	dd = 1 - (pnl / water_mark(pnl, how='high').shift(1))
	dd.ix[dd < 0] = 0
	return dd


def drawdown_dur(pnl):
	"""Accumulative Drawdown Duration

	Parameter
	---------
	pnl: Pandas Series 
		- index of timestamp
		- period percentagized returns


	Theory
	------
	1. Get the Drawdown percentage series
	2. Convert anything that is not 0 to 1 as boolean type
	3. Then find periodic drawdown period
		- by comparising on changing in boolean value
		- cumsum to get a series that labeled with drawdown period
	4. Group by each drawdown period using count size

	Rerturn
	-------
	Integer, maximum period of drawdown period length
	"""
	# Get Drawdown Max Duration
	sr = drawdown(pnl)[1:].astype(bool)
	return sr.groupby((sr != sr.shift()).cumsum()).size()


def max_drawdown_dur(pnl):
	# Get Drawdown Max Duration
	return drawdown_dur(pnl).max()


def max_drawdown(pnl):
	"""Max drawdown Percentage in the trading period

	Parameter
	---------
	pnl: Pandas Series 
		- index of timestamp
		- period percentagized returns

	Rerturn
	-------
	FLoat, maximum drawdown percentage
	"""
	return drawdown(pnl).max()


def lpm(returns, threshold=0, order=2):
	"""lower/Downside partial moment of the returns"""
	r = np.asarray(returns)
	diff = (threshold - r).clip(min=0)
	return ((diff**order).sum() / diff.shape[0])**(1/order)

	
def hpm(returns, threshold=0, order=2):
	"""Higher/Upside partial moment of the returns"""
	r = np.asarray(returns)
	diff = (r - threshold).clip(min=0)
	return ((diff**order).sum() / diff.shape[0])**(1/order)


def omega_ratio(returns, target=0, N=250):
	"""Excess Return per unit of Semi-Downside-Mean"""
	er = np.mean(returns)
	rf = (Constant.Rf + 1) ** (1/N) - 1
	return N * (er - rf) / lpm(returns, target, 1)


def sortino_ratio(returns, target=0, N=250):
	"""Excess Return per unit of Semi-Downside-Stdev"""
	er = np.mean(returns)
	rf = (Constant.Rf + 1) ** (1/N) - 1
	return (N**0.5) * (er - rf) / lpm(returns, target, 2)


def kappa_three_ratio(returns, target=0, N=250):
	"""Excess Return per unit of Semi-Downside-Skewness"""
	er = np.mean(returns)
	rf = (Constant.Rf + 1) ** (1/N) - 1
	return (N**(1/3)) * (er - rf) / lpm(returns, target, 3)

 
def gain_loss_ratio(returns, target=0):
	"""Upside Performnace vs. Downside Performance

	Lower than 1: Upside movement is less than Downside movement
	Higher than 1: Upside movement is less than Downside movement
	"""
	return hpm(returns, target, 1) / lpm(returns, target, 1)

 
def upside_potential_ratio(returns, target=0):
	"""Upside Performance per unit of Downside Risk

	Lower than 1: Per unit time Upside performance
		is usually lower than downside risk
	Higher than 1: Per unit time Upside performance
		is usually higher than downside risk
	"""
	return hpm(returns, target, 1) / lpm(returns, target, 2)


def dd_adj_ratio(max_dd, max_dd_dur, avg_dd, avg_dd_dur, N=250):
	"""Adjusting Ratio that considers Drawdown and Duration
	It is the sum between 
	- drawdown's max & avg harmonic mean
	- drawdown's max & avg duration in respect to N harmonic mean

	This is used to adjust ratio to consider Drawdown impacts
	"""
	dd = 2/(1/avg_dd + 1/max_dd)
	dur = 2/(1/(avg_dd_dur**1.5/N) + 1/(max_dd_dur**1.5/N))
	return max(1 - (dd+dur), 0)


def risk_adj_ratio(gl, upside, sortino):
	"""Adjusted Risk Measurement
	- Higher Gain Loss Ratio signals more winning days
	- Lower Upside potential signals worse downside risk
	- Sortino is the overall reward per unit of downside risk

	Return
	------
	The product of three ratio
	"""
	return gl * upside * sortino


def bench_adj_return(alpha, beta, cagr):
	"""Portfoli/Strategy Benchmark Adjusted Return
	- Lower beta means less volalital then the market
	- Higher alpha signals better abnormal return
	- CAGR gives the overall reutrn
	- Lower beta signals less risky abnormal return

	Return
	------
	The harmonic mean between adjusted alpha and CAGR
	"""
	return 2 / (1/(alpha * 1/beta) + 1/cagr)


def sigmoid(x):
	return 1 / (1 + np.e**(-1 * x))

def tanh(x):
    return (1-np.e**(-2*x)) / (1+np.e**(-2*x))


def pcm_ratio(pnl, target=0, N=250):
	"""PCM Home Grown Risk Metrics
	- The sigmoid normalized
	- log of product between risk adjusted ratio and drawdown adjusted ratio
	- Range -1 ~ 1, no risk adjusted return = 0

	Parameter
	--------
	pnl: equity curve
	target: target return / Min Acceptable Return, default=0
	N: Number of unit in one Year
	"""
	returns = pd.Series(pnl).pct_change().fillna(0)

	sortino = sortino_ratio(returns, N=N, target=target)
	gl = gain_loss_ratio(returns, target=target)
	upside = upside_potential_ratio(returns, target=target)

	max_dd = max_drawdown(pnl)
	max_dd_dur = max_drawdown_dur(pnl)
	avg_dd = drawdown(pnl).mean()
	avg_dd_dur = drawdown_dur(pnl).mean()

	risk_adj = risk_adj_ratio(gl, upside, sortino)
	dd_adj = dd_adj_ratio(max_dd, max_dd_dur, avg_dd, avg_dd_dur, N=N)

	x = risk_adj * dd_adj
	if x <= 0:
		return -1
	else:
		return tanh(np.log(x))
