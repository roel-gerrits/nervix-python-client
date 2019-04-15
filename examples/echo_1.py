import logging
import nervix

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s"
)


def on_call(call):
    print("Received", call)
    call.post(call.payload)


if __name__ == '__main__':
    loop, channel = nervix.create_channel("nxtcp://localhost:9999")

    sess = channel.session('demo', standby=True)
    sess.add_call_handler(on_call)

    loop.run_forever()
