import logging
import os
import pwd
import sys
from collections import deque

import forklib
from aiomisc.entrypoint import entrypoint
from aiomisc.log import LogFormat, basic_config
from aiomisc.utils import bind_socket
from configargparse import ArgumentParser
from setproctitle import setproctitle
from yarl import URL

from jaeger_proxy.rest import API
from jaeger_proxy.sender import Sender

log = logging.getLogger()

parser = ArgumentParser(auto_env_var_prefix="APP_")

parser.add_argument("-f", "--forks", type=int, default=4)
parser.add_argument(
    "-u", "--user", help="Change process UID", type=pwd.getpwnam
)
parser.add_argument("-D", "--debug", action="store_true")

parser.add_argument(
    "--log-level",
    default="info",
    choices=("debug", "info", "warning", "error", "fatal"),
)

parser.add_argument(
    "--log-format", choices=LogFormat.choices(), default="color"
)

parser.add_argument("--pool-size", default=4, type=int)

group = parser.add_argument_group("HTTP settings")
group.add_argument("--http-address", type=str, default="0.0.0.0")
group.add_argument("--http-port", type=int, default=8080)
group.add_argument("--http-password", type=str, required=True)
group.add_argument("--http-login", type=str, default="admin")

group = parser.add_argument_group("Jaeger settings")
group.add_argument("--jaeger-route", type=URL, required=True)

group = parser.add_argument_group("Sender settings")
parser.add_argument("--sender-interval", default=1, type=float,
                    help="interval to send in seconds")


def main():
    arguments = parser.parse_args()
    os.environ.clear()

    basic_config(
        level=arguments.log_level,
        log_format=arguments.log_format,
        buffered=False,
    )

    setproctitle(os.path.basename("[Master] %s" % sys.argv[0]))

    sock = bind_socket(
        address=arguments.http_address, port=arguments.http_port
    )
    queue = deque()
    services = [
        API(
            sock=sock,
            password=arguments.http_password,
            login=arguments.http_login,
            queue=queue,
        ),
        Sender(
            jaeger_route=arguments.jaeger_route,
            interval=arguments.sender_interval,
            queue=queue,
        ),
    ]

    if arguments.user is not None:
        logging.info("Changing user to %r", arguments.user.pw_name)
        os.setgid(arguments.user.pw_gid)
        os.setuid(arguments.user.pw_uid)

    def run():
        setproctitle(os.path.basename("[Worker] %s" % sys.argv[0]))

        with entrypoint(
            *services,
            pool_size=arguments.pool_size,
            log_level=arguments.log_level,
            log_format=arguments.log_format,
            debug=arguments.debug
        ) as loop:
            loop.run_forever()

    if arguments.forks:
        forklib.fork(arguments.forks, run, auto_restart=True)
    else:
        run()


if __name__ == "__main__":
    main()
