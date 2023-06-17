import pandas as pd
import numpy as np


def show_new_cols(source_df, target_df):
    records_df2 = set([tuple(row) for row in source_df.values])
    in_df2_mask = np.array([tuple(row) in records_df2 for row in target_df.values])
    result = target_df[~in_df2_mask]
    return result

