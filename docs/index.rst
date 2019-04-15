
python-nervix
=========================

The nervix interface for python programs.

Nervix is a message broker. Your software can talk to a nervix server in
a number of ways. This library's goal is to provide the easiest way to
let your python projects talk with nervix.

See the nervix project for detailed information about what nervix is and how it works.

.. todo:: include link to nervix server project website

Usage example
-------------

A minimal application that issues a request and handles its response may look like this::

    from nervix import create_channel

    loop, channel = create_channel('nxtcp://localhost:9999')

    def response_handler(msg):
        print(msg)

    req = channel.request('demo_target', 'demo_payload')
    req.set_handler(response)
    req.send()

    loop.run_forever()


Features
--------

* **Easy to use**. The API is 'simple as', and is designed to stay out of your way.

* **Automatic reconnection**. This library doesn't panic when the connection to the
  server is spotty, it will automaticly reconnect if needed, and your application
  won't notice a thing.

* **Flexible serialization**. As with all messaging systems, your objects have to be turned into bytes at some point in
  order to send them over the wire. This library provides a basic serializer as a default
  but lets you plug in your own serializers if desired.

Read more
-------------

.. toctree::
   :maxdepth: 2

   installation
   usage
   apidoc