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

import os
import time
import logging
import subprocess
import kinetic
from kinetic import zero_copy

LOG = logging.getLogger(__name__)

transfered = 0

def zc_write(hostname, path, port=8123, value_size=1024*1024):
    c = kinetic.AsyncClient(hostname, port)
    c.connect()


    fd = open(path, 'r')
    total_size = os.path.getsize(path)

    def build_success(size):
        def on_success(m):
            global transfered
            transfered += size
            LOG.info('Bytes transfered: %s' % transfered)
        return on_success

    def on_error(ex): LOG.error(ex)

    t1 = time.time()

    left = total_size
    i = 0
    while left > 0:
        if left > value_size: ln = value_size
        else: ln = left

        c.putAsync(build_success(ln), on_error, 'key' + str(i),
                   zero_copy.ZeroCopyValue(fd, None, ln),
                   synchronization=kinetic.common.Synchronization.WRITEBACK)

        left -= ln
        i += 1

    c.wait()
    t2 = time.time()

    tmb = transfered / (1024.*1024.)
    LOG.info("Transfered {0:.2f} MB in {1:.2f} seconds ({2:.2f} MB/s)"
             .format(tmb, (t2 - t1), tmb / (t2 - t1)))

    c.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Kinetic example (zero-copy write)')
    parser.add_argument('address', metavar='address',
                       help='Target drive address')
    parser.add_argument('path', metavar='path',
                       help='Path to file to transfer')
    parser.add_argument('--port', dest='port', type=int, default=8123,
                       help='Port listening on kinetic (default=8123)')
    parser.add_argument('--chunksize', dest='chunk_size', type=int, default=1024*1024,
                       help='Size of chunks in bytes to split the input into (default is 1 MB)')
    parser.add_argument('--log', dest='loglevel', default="info",
                       help='Logging level (default=info)')

    args = parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(format='%(asctime)-8s %(levelname)s: %(message)s',
                        datefmt="%H:%M:%S", level=numeric_level)

    zc_write(args.address, args.path, port=args.port, value_size=args.chunk_size)

if __name__ == '__main__':
    main()
