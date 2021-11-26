from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

import argparse
import os


def main(results_dir):
    bw_files = Path(results_dir).rglob('bw.csv')

    agg_df = pd.DataFrame()
    for f in bw_files:
       df = pd.read_csv(f)
       df.drop('pgpgin/s', axis=1, inplace=True)
       df.rename({'pgpgout/s':'pgout/s', 'majflt/s':'pgin/s'},
               axis=1, inplace=True)
       df = df.max().to_frame().T
       df['system'] = str(os.path.basename(f.parent))
       df['pgout/s'] = df['pgout/s'].div(4)
       agg_df = pd.concat([agg_df, df])

    agg_df = agg_df.melt(['system'], var_name='type',
            value_name='throughput')

    g = sns.barplot(x='type', y='throughput', hue='system',
            palette='rocket', data=agg_df)
    g.set(xlabel=None)
    g.set(ylabel='Throughput (#pages/s)')

    for p in g.patches:
        g.annotate(format(round(p.get_height()/1000), '.0f')+"K",
           (p.get_x() + p.get_width() / 2., p.get_height()),
           ha = 'center', va = 'center', size=15, xytext = (0, -12),
           textcoords = 'offset points')
    plt.savefig('bw_plot.pdf')

    print(agg_df)

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--results-dir',
            help='directory where result files are stored', required=True)
    args = parser.parse_args()
    main(args.results_dir)
