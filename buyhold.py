from .conf import LONG, SHORT
from .strategy import BaseStrategy


class BuyAndHold(BaseStrategy):
	"""
	Goes LONG all of the symbols as soon as a tick is received.
	It will never exit the position

	Primarily used as a testing mechanisms for the Strategy class
	as well as benchmark to compare other strategies
	"""
	def __init__(self, symbol_list, allocation, direction=LONG, **kws):
		super().__init__(symbol_list=symbol_list, allocation=allocation, **kws)
		self.hard_stopped = {s: False for s in symbol_list}
		self.direction = direction


	def calculate_signals(self):
		"""'Buy and Hold' strategy
		- generate signal for each symbol
		- then no more addition signal
		- means we constantly long the market since initialization

		Parameters
		----------
		event: A MarketEvent object
		"""
		for s in self.pos:
			if self.hard_stopped[s]: continue
			if self.has_position(s): continue
				
			self.generate_signal(s, self.direction)


	def on_hard_stop(self, symbol):
		self.hard_stopped[symbol] = True
