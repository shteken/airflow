#!/usr/bin/env python3

import sys
import os
import pandas as pd

output = sys.argv[2]
input_file = sys.argv[1]


df = pd.read_csv(input_file)
calculated_df = df.groupby(['diagnosis']).mean()
calculated_df.to_csv(output)