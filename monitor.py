import pandas as pd, numpy as np, logging
import matplotlib.pyplot as plt, seaborn as sns

from matplotlib.ticker import FuncFormatter
from collections import OrderedDict
from cytoolz import pluck

from .util import dollar_trunc
from .metrics import (
	drawdown, max_drawdown_dur, max_drawdown, drawdown_dur, sharpe_ratio,
	roy_safety_ratio, alpha_beta, var_cov_var, sortino_ratio, omega_ratio,
	gain_loss_ratio, upside_potential_ratio, pcm_ratio, bench_adj_return
)

logger = logging.getLogger('Monitor')



class StaticMonitor:
	def __init__(self, stgy, benchmark=['416904']):
		self._stgy = stgy
		self._benchmark = benchmark

		# palattets for the report
		self._palatte = sns.xkcd_palette([
			'windows blue','amber','magenta','teal','scarlet'
		])


	def create_curves(self):
		"""Creates a pandas DataFrame from the 'all_holdings'
		list of dictionaries

		Return
		------
		self.equity_curve: Attribute with in 'all_holdings'
		"""
		curve = pd.DataFrame(self.all_holdings)
		curve['returns'] = curve['total'].pct_change()
		curve['equity_curve'] = (1 + curve['returns']).cumprod()
		curve['drawdown'] = drawdown(curve['equity_curve'])
		self.equity_curve = curve[curve.datetime.notnull()].set_index('datetime')

		ben = pd.DataFrame(self.all_benchmark).set_index('datetime')
		ben = (1 + ben.pct_change().fillna(0)).cumprod()
		self.ben_curve = ben


	def _validate_equity_curve(self):
		if not hasattr(self, 'equity_curve') or not hasattr(self, 'ben_curve'):
			self.create_curves()
		return self.equity_curve, self.ben_curve


	def output_summary_stats(self, N=250):
		"""
		Create a list of summary statistics for the portfolio

		Parameter
		---------
		N: int, default to 250
			number of market event in one Year in order to have
			all statistics to be annualized
		"""
		curve, ben = self._validate_equity_curve()

		total_return = curve['equity_curve'][-1]  # return to date
		er = total_return ** (1/curve.shape[0]) - 1  # expected daily return
		cagr = (1 + er) ** N - 1   # annualized return
		returns = curve['returns']
		pnl = curve['equity_curve']
		dd = curve['drawdown']
		ben = ben.iloc[:,0].pct_change().fillna(0)

		sharpe = sharpe_ratio(er, returns, N=N)
		alpha, beta = alpha_beta(returns, ben, N=N)
		bench_adj = bench_adj_return(alpha, beta, cagr)
		sortino = sortino_ratio(er, returns, N=N, target=0)
		gl = gain_loss_ratio(returns, target=0)
		upside = upside_potential_ratio(returns, target=0)

		stats = OrderedDict([
			('Total Return', '%0.2f%%' % ((total_return-1)*100)),
			('CAGR', '%0.2f%%' % (cagr*100)),
			('Alpha', '%0.2f%%' % (alpha*100)),
			('Beta', '%0.2f' % (beta)),
			('Benchmark Adjusted Return', '%0.2f%%' % (bench_adj*100)),

			('Sharpe Ratio', '%0.2f' % (sharpe)),
			('Sortino Ratio', '%0.2f' % (sortino)),
			('Gain Loss Ratio', '%0.2f' % (gl)),
			('Upside Potential Ratio', '%0.2f' % (upside)), 
			('Max Drawdown', '%0.2f%%' % (max_drawdown(pnl) * 100)),
			('Max Drawdown Duration', '%0.2f days' % (max_drawdown_dur(pnl)/(N/250))),
			('Avg Drawdown', '%0.2f%%' % (dd.mean() * 100)),
			('Avg Drawdown Duration', '%0.2f days' % (drawdown_dur(pnl).mean()/(N/250))),
			('PCM Ratio', '%0.2f' % (pcm_ratio(pnl, returns, N=N))),
		])
		return stats


	def plot_equity_curve(self):
		curve, ben = self._validate_equity_curve()
		
		with sns.axes_style('ticks'):
			fig, (ax1, ax2, ax3, ax4) = plt.subplots(4,1, figsize=(10,12), sharex=True)

			ben.plot(color=self._palatte[1], lw=1, alpha=0.7, ax=ax1)
			sr = curve['equity_curve']
			sr.plot(color=self._palatte[3], lw=2, ax=ax1)
			ax1.axhline(y=1, color='grey', linestyle='--', linewidth=2)
			ax1.yaxis.set_major_formatter(FuncFormatter(
				lambda x,p: '{:0,.0f}'.format(x*100)
			))
			ax1.set_ylabel('Portfolio Value %')


			curve['returns'].plot(ax=ax2, color='grey', marker='o', lw=0.3, ms=3)
			ax2.yaxis.set_major_formatter(FuncFormatter(
				lambda x,p: '{:0,.1f}'.format(x*100)
			))
			ax2.set_ylabel('Period Returns %')


			curve['drawdown'].plot(ax=ax3, color=self._palatte[4], lw=1)
			ax3.yaxis.set_major_formatter(FuncFormatter(
				lambda x,p: '{:0,.1f}'.format(x*100)
			))
			ax3.set_ylabel('Drawdowns %')


			curve['buying_power'].plot(color=self._palatte[3], lw=1, ax=ax4)
			ax4.axhline(y=0, color='grey', linestyle='--', linewidth=2)
			ax4.yaxis.set_major_formatter(FuncFormatter(
				lambda x,p: dollar_trunc(x, decimal=0, dollar_sign=True)
			))
			ax4.set_ylabel('Buying Power')

		for ax in fig.axes:
			sns.despine(ax=ax, top=True, right=True, bottom=False, left=False)

		return fig
