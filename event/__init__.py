from .core import Event
from .market import MarketEvent, Tick
from .signal import SignalEventFixed, SignalEventPct
from .order import OrderEvent
from .fill import FillEvent, FillEventIB


EVENT_MAP = {
	'market': MarketEvent,
	'order': OrderEvent,
	'fill_ib': FillEventIB,
	'signal_fixed': SignalEventFixed,
	'signal_pct': SignalEventPct,
}
