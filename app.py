from __future__ import unicode_literals, absolute_import

import sys

from bbrother.main import TrackAll
from bbrother.config import config


if __name__ == '__main__':
    app = TrackAll(config)
    sys.exit(app.run())
