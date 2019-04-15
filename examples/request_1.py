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

    timer = loop.timer()


    def trigger():
        req = channel.request('demo', 'hello world')
        print("trigger")
        # req.set_handler(response)
        req.send()
        timer.set(.01)


    timer.set_handler(trigger)
    timer.set(.2)

    loop.run_forever()
