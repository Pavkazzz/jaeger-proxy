from asyncio import gather
from collections import deque

from aiohttp import ClientSession
from aiomisc.service.periodic import PeriodicService
from yarl import URL


class Sender(PeriodicService):
    __required__ = "jaeger_route", "interval", "queue"

    jaeger_route: URL
    interval: float
    queue: deque

    bulk_size: int = 10000
    session: ClientSession = None

    async def callback(self):
        if not self.queue:
            return

        metrics = []

        while self.queue or len(metrics) < self.bulk_size:
            try:
                metrics.append(self.queue.popleft())
            except IndexError:
                break

        await self.send(metrics)

    async def stop(self, *args, **kwargs):
        await super().stop(*args, **kwargs)

        metrics = list(self.queue)
        self.queue.clear()
        await self.send(metrics)

    async def send(self, metrics):
        if not metrics:
            return
        async with ClientSession() as conn:
            print(self.jaeger_route, "metrics", metrics)
            await gather(
                *[
                    conn.post(self.jaeger_route, data=data, headers=headers)
                    for data, headers in metrics
                ]
            )
