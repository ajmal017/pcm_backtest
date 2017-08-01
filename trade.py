import numpy as np

from bson import ObjectId
from copy import copy
from collections import deque
from .errors import OverFilling



class Trade:
	__slots__ = [
		't', 'position', 'open_quantity', 'quantity',
		'realized', 'cost', 'max_cost', 'max_profit',
		'orders', 'share_queue', 'tick', 'id',
	]

	"""A Trade Position when created up on opening order"""
	def __init__(self, oid, quantity, direction, tick):
		self.t = 1
		self.position = direction  # 1 for LONG, -1 for SHORT
		self.open_quantity = 0
		self.quantity = 0

		self.realized = 0
		self.cost = 0
		self.max_cost = 0
		self.max_profit = 0

		self.orders = {}
		self.share_queue = deque()
		self.tick = tick

		self.on_order(oid, quantity, direction)
		self.id = ObjectId()


	def __repr__(self):
		return 'Trade<{} | id={}, t={}, Q={}, profit={}, r={}, closed={}>'.format(
			self.position, self.id, self.t, self.quantity, 
			self.profit, self.r, self.is_closed
		)


	@property
	def has_open_orders(self):
		return len(self.orders) != 0

	@property
	def cost_basis(self):
		try:
			return self.cost / self.quantity
		except ZeroDivisionError:
			return 0

	@property
	def r(self):
		try:
			return self.profit / self.max_cost
		except ZeroDivisionError:
			return -np.inf

	@property
	def profit(self):
		return self.unrealized + self.realized

	@property
	def drawdown(self):
		try:
			return self.max_profit / self.profit - 1
		except ZeroDivisionError:
			return 0

	@property
	def total_quantity(self):
		return self.quantity + self.open_quantity

	@property
	def is_closed(self):
		"""Trade should alwaysed opend with a order
		- thus the opening quantity should never be 0
		- Once it reached 0, and there is no quantity left, too
		- the trade is consider as closed
		"""
		return (
			self.quantity == 0
			and self.open_quantity == 0
			and len(self.orders) == 0
		)

	@property
	def is_closing(self):
		return self.total_quantity == 0 and len(self.orders) != 0

	@property
	def mv(self):
		return self.position * self.tick.close * self.quantity

	@property
	def unrealized(self):
		return (
			self.position * self.quantity
			* (self.tick.close - self.cost_basis)
		)


	def as_dict(self):
		return {
			't': self.t, 'position': self.position, 'mv': self.mv,
			'open_quantity': self.open_quantity, 'quantity': self.quantity,
			'realized': self.realized, 'unrealized': self.unrealized,
			'cost': self.cost, 'max_cost': self.max_cost,
			'cost_basis': self.cost_basis,
			'r': self.r, 'profit': self.profit, 'max_profit': self.max_profit,
			'drawdown': self.drawdown, 'is_closed': self.is_closed,
		}


	def on_market(self, tick):
		self.tick = tick
		price = tick.close

		self.t += 1
		self.max_profit = max(self.profit, self.max_profit)


	def on_order(self, oid, quantity, direction):
		self.open_quantity += self.position * direction * quantity
		self.orders[oid] = {'Q': quantity, 'D': direction}


	def on_fill(self, oid, quantity, direction, cost, commission):
		need_fill = quantity

		if direction == self.position:  # opening more
			cps = cost + self.position * commission/quantity
			shares = {'Q': quantity, 'C': cps}

			while need_fill > 0:
				try:
					order = copy(self.orders[oid])
				except KeyError:
					raise OverFilling('No matching order')

				Q = min(order['Q'], need_fill)
				order['Q'] -= Q
				need_fill -= Q
				self.open_quantity -= Q
				self.quantity += Q

				if order['Q'] != 0:
					self.orders[oid] = order
				else:
					self.orders.pop(oid)

			self.cost += shares['C'] * Q
			self.max_cost = max(self.cost, self.max_cost)
			self.share_queue.append(shares)

		else:  # closing
			cps = cost - self.position * commission/quantity
			
			while need_fill > 0:
				try:
					order = copy(self.orders[oid])
					shares = self.share_queue.popleft()
				except KeyError:
					raise OverFilling('No maching order')
				except IndexError:
					raise OverFilling('No more shares to be closed')

				Q = min(shares['Q'], need_fill)
				shares['Q'] -= Q
				order['Q'] -= Q
				need_fill -= Q

				self.cost -= Q * shares['C']
				self.realized += self.position * Q * (cps - shares['C'])
				self.open_quantity += Q
				self.quantity -= Q

				if shares['Q'] != 0:
					self.share_queue.appendleft(shares)

				if order['Q'] != 0:
					self.orders[oid] = order
				else:
					self.orders.pop(oid)
