import logging
import nervix

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s"
)


def response(message):
    print(message)


if __name__ == '__main__':
    loop, channel = nervix.create_channel("nxtcp://localhost:9999")

    req = channel.request('demo', 'hello world')
    req.set_handler(response)
    req.send()

    loop.run_forever()
