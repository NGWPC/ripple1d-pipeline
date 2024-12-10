import pandas as pd

from .purge import delete_reach_data
from .run_flows2fim import run_flows2fim
from .utils import *

# Allows displaying the full content in cells
pd.set_option("display.max_colwidth", None)  
# Display all rows
# pd.set_option('display.max_rows', None)    
pd.set_option("display.max_columns", None)
