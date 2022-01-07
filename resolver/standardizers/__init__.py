
import os
from pathlib import Path

RESOURCES = Path(os.path.dirname(os.path.abspath(__file__))) / 'resources'

from resolver.standardizers.custom_lookup import UsState2Code

__all__ = ['UsState2Code']
