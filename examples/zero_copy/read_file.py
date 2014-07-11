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
from kinetic import zero_copy
import time
import traceback

LOG = logging.getLogger(__name__)

transfered = 0

def zc_read(hostname, port=8123, path=None, warmup=True, keys_to_read=100,
            warmup_keys=10, warmup_size=1024*1024):

    c = kinetic.AsyncClient(hostname, port, defer_read=True)
    c.connect()

    if warmup:
        LOG.info("Warming up...")
        data = bytearray('f' * warmup_size)
        c2 = kinetic.Client(c.hostname, chunk_size=1024*1024)
        for i in range(0, warmup_keys):
            c2.put('key' + str(i), data)
        LOG.info("Warmup finished.")
        c2.close()

    if path:
        fd_out = open(path, 'wb+')
    else:
        import sys
        fd_out = sys.stdout

    t1 = time.time()

    def on_success(entry):
        global transfered
        if entry:
            try:
                zero_copy.forwardto(entry.value, fd_out)
                transfered += entry.value.length
                LOG.info('Bytes transfered: %s' % transfered)
            except:
                traceback.print_exc()

    def on_error(ex): print ex

    for i in range(0, keys_to_read):
        c.getAsync(on_success, on_error, 'key' + str(i % warmup_keys))

    c.wait()
    t2 = time.time()
    mb_trans = transfered / (1024.*1024.)
    LOG.info('Transfered {0:.2f} MBs in {1:.2f} seconds ({2:.2f} MB/s)'
             .format(mb_trans, t2 - t1, mb_trans / (t2 - t1)))

    c.close()

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Kinetic example (zero-copy read)')
    parser.add_argument('address', metavar='address',
                       help='Target drive address')
    parser.add_argument('--port', dest='port', type=int, default=8123,
                       help='Port listening on kinetic (default=8123)')
    parser.add_argument('--output', dest='output', default=None,
                       help='Output content to file (default is stdout)')
    parser.add_argument('--log', dest='loglevel', default="info",
                       help='Logging level (default=info)')

    args = parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(format='%(asctime)-8s %(levelname)s: %(message)s',
                        datefmt="%H:%M:%S", level=numeric_level)

    zc_read(args.address, port=args.port, path=args.output)

if __name__ == '__main__':
    main()
