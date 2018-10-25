from __future__ import unicode_literals, absolute_import

import sys

from trackall.main import TrackAll
from trackall.config import config


if __name__ == '__main__':
    app = TrackAll(config)
    sys.exit(app.run())
