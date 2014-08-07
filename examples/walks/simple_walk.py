# Copyright (c) 2014 Seagate Technology

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

#@author: Ignacio Corderi

import logging
import kinetic

LOG = logging.getLogger(__name__)

def example(address, port, from_key, to_key):

    c = kinetic.AsyncClient(address, port)
    c.connect()

    def walk(from_key=None, to_key=None,
             startKeyInclusive=True,
             endKeyInclusive=True,
             maxReturned=200):
        if not from_key:
            from_key = ''
        if not to_key:
            to_key = '\xFF' * kinetic.common.MAX_KEY_SIZE
        keys = c.getKeyRange(from_key, to_key,
                             startKeyInclusive, endKeyInclusive, maxReturned)
        if len(keys) > 0:
            for k in keys:
                yield k
            for k in walk(keys[-1], to_key,
                          False, endKeyInclusive, maxReturned):
                yield k

    for x in walk(from_key, to_key):
        print x

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Kinetic Walk Example')
    parser.add_argument('address', metavar='address',
                       help='Target drive address')
    parser.add_argument('--port', dest='port', type=int, default=8123,
                       help='Port listening on kinetic (default=8123)')
    parser.add_argument('--from', dest='from_key', default=None,
                       help='From key (default is empty)'),
    parser.add_argument('--to', dest='to_key', default=None,
                       help='To key (default is last possible key)'),
    parser.add_argument('--log', dest='loglevel', default="info",
                       help='Logging level (default=info)')

    args = parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(format='%(asctime)-8s %(levelname)s: %(message)s',
                        datefmt="%H:%M:%S", level=numeric_level)

    example(args.address, args.port, args.from_key, args.to_key)

if __name__ == '__main__':
    main()
