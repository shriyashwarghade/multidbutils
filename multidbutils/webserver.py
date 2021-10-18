import argparse

import uvicorn

from multidbutils._webserver.app import app


def main():
    parser = argparse.ArgumentParser(description="Run py-multi-db-utility Webserver")

    parser.add_argument("ip", nargs='?', metavar="ip", type=str,
                        help="Ip Address To Run Server On (Default: localhost).", default="localhost")

    parser.add_argument("port", nargs='?', metavar="port", type=int,
                        help="Port To Run Server On (Default: 8000).", default=8000)

    args = parser.parse_args()

    uvicorn.run(app, host=args.ip, port=int(args.port), )


if __name__ == "__main__":
    main()
