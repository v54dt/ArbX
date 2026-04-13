from abc import ABC, abstractmethod
from typing import Callable

from src.models.messages import Fill, OrderRequest, OrderResponse, Quote


class BaseAdapter(ABC):
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def subscribe_quotes(
        self, symbols: list[str], callback: Callable[[Quote], None]
    ) -> None: ...

    @abstractmethod
    async def place_order(self, request: OrderRequest) -> OrderResponse: ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool: ...

    @abstractmethod
    async def get_positions(self) -> list[dict]: ...
