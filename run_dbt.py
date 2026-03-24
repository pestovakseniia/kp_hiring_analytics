import sys
import subprocess
import os
from dotenv import load_dotenv

base_dir = os.path.dirname(os.path.abspath(__file__))

# Load .env
load_dotenv(os.path.join(base_dir, ".env"))

# Always point to dbt project
project_dir = os.path.join(base_dir, "hiring_analytics")

cmd = ["dbt"] + sys.argv[1:] + ["--project-dir", project_dir]

subprocess.run(cmd)