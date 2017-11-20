from spyne.application import Application

from spyne.decorator import srpc
from spyne.service import ServiceBase
from spyne.model.complex import Iterable
from spyne.model.primitive import UnsignedInteger
from spyne.model.primitive import String
from spyne.server.wsgi import WsgiApplication
from spyne.protocol.soap import Soap11
from wsgiref.simple_server import make_server
import logging

class HelloWorldService(ServiceBase):
    @srpc(String, UnsignedInteger, _returns=Iterable(String))
    def say_hello(name, times):
        for i in xrange(times):
            yield 'Hello, %s' % name

    @srpc(_returns=String)
    def hello_world():
        return 'Hello world!'

if __name__=='__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)

    app = Application([HelloWorldService], 'spyne.examples.hello.http',
        in_protocol=Soap11(validator='lxml'),
        out_protocol=Soap11(),
    )

    wsgi_app = WsgiApplication(app)

    server = make_server('127.0.0.1', 7789, wsgi_app)

    print("listening to http://127.0.0.1:7789")
    print("wsdl is at: http://localhost:7789/?wsdl")

    server.serve_forever()