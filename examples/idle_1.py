import logging
import nervix

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s"
)

if __name__ == '__main__':
    loop, channel = nervix.create_channel("nxtcp://localhost:9999")

    loop.run_forever()
