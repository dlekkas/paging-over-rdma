from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import argparse
import os

sns.set(style='whitegrid')
sns.set_palette(sns.color_palette('bright'))

def main(results_dir):
    app_latency_files = Path(results_dir).rglob('app_exec_times.csv')

    agg_df = pd.DataFrame()
    for f in app_latency_files:
        rep_df = pd.read_csv(f, names=['latency', 'memory'])
        latency_df = rep_df.groupby('memory', as_index=False)['latency'].median()
        latency_df['system'] = str(os.path.basename(f.parent))
        agg_df = pd.concat([agg_df, latency_df])

    agg_df['norm_runtime'] = agg_df.apply(
            lambda x: x['latency'] / agg_df.loc[(agg_df.system=='noswap') &
                (agg_df.memory==x['memory']), 'latency'].values[0], axis=1)
    agg_df['rmem_ratio'] = (agg_df.memory - 280) / agg_df.memory
    agg_df['rmem_ratio'] = agg_df['rmem_ratio'].apply(lambda x: "{0:.2f}%".format(x*100))

    agg_df.reset_index(drop=True, inplace=True)
    ax = sns.lineplot(data=agg_df, x='rmem_ratio', y='norm_runtime', hue='system',
            style='system', markers=True, dashes=False)
    ax.set(xlabel='Remote memory fraction', ylabel='Normalized runtime',
           title='Dummy application execution time comparison')
    ax.get_figure().savefig('app_runtime_plot.pdf')


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--results-dir',
            help='directory where result files are stored', required=True)
    args = parser.parse_args()
    main(args.results_dir)
