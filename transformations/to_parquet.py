#!/usr/bin/env python3

import sys
import pandas as pd

output = sys.argv[2]
input_file = sys.argv[1]


df = pd.read_csv(input_file)
df.to_parquet(output)