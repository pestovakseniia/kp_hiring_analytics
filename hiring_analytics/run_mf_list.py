import sys
import subprocess
import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

# subprocess.run(["mf", "list", "metrics"])
subprocess.run(['mf', 'list', 'saved-queries'])