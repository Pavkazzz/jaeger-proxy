import logging
from collections import deque
from http import HTTPStatus

from aiohttp import BasicAuth, hdrs
from aiohttp.web import (
    Application,
    HTTPForbidden,
    HTTPUnauthorized,
    Request,
    Response,
)
from aiomisc.service.aiohttp import AIOHTTPService

from jaeger_proxy import version

log = logging.getLogger(__name__)


async def ping(request: Request):
    return Response(
        headers=dict(version=version.__version__),
        content_type="text/plain",
        status=HTTPStatus.OK
    )


BYPASS_HEADERS = [
    hdrs.ACCEPT_ENCODING,
    hdrs.CONTENT_LENGTH,
    hdrs.CONTENT_TYPE,
    hdrs.USER_AGENT,
]


async def statistic_receiver(request: Request):
    auth = request.headers.get("Authorization")

    if not auth:
        raise HTTPUnauthorized
    try:
        basic = BasicAuth.decode(auth)
    except ValueError:
        log.exception("Failed to parse basic auth")
        raise HTTPForbidden

    if request.app["password"] != basic.password:
        raise HTTPForbidden

    if request.app["login"] != basic.login:
        raise HTTPForbidden

    data = await request.read()

    bypass_headers = {
        header: request.headers.get(header)
        for header in BYPASS_HEADERS
        if header
    }
    request.app['queue'].append((data, bypass_headers))

    return Response(content_type="text/plain", status=HTTPStatus.ACCEPTED)


class API(AIOHTTPService):
    __required__ = "password", "login", "queue"

    password: str
    login: str
    queue: deque

    @staticmethod
    async def setup_routes(app: Application):
        router = app.router  # type: UrlDispatcher
        router.add_get("/ping", ping)
        router.add_post("/api/traces", statistic_receiver)

    async def create_application(self) -> Application:
        app = Application()
        app.on_startup.append(self.setup_routes)
        app["password"] = self.password
        app["login"] = self.login
        app["queue"] = self.queue
        return app
