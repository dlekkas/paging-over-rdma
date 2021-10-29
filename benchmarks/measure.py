from beautifultable import BeautifulTable
import matplotlib.pyplot as plt
import numpy as np
import math
import argparse

def outlier_aware_hist(data, lower=None, upper=None):
    if not lower or lower < data.min():
        lower = data.min()
        lower_outliers = False
    else:
        lower_outliers = True

    if not upper or upper > data.max():
        upper = data.max()
        upper_outliers = False
    else:
        upper_outliers = True

    n, bins, patches = plt.hist(data, range=(lower, upper), bins='auto')

    if lower_outliers:
        n_lower_outliers = (data < lower).sum()
        patches[0].set_height(patches[0].get_height() + n_lower_outliers)
        patches[0].set_facecolor('c')
        patches[0].set_label('Lower outliers: ({:.2f}, {:.2f})'.format(data.min(), lower))

    if upper_outliers:
        n_upper_outliers = (data > upper).sum()
        patches[-1].set_height(patches[-1].get_height() + n_upper_outliers)
        patches[-1].set_facecolor('m')
        patches[-1].set_label('Upper outliers: ({:.2f}, {:.2f})'.format(upper, data.max()))

    if lower_outliers or upper_outliers:
        plt.legend()


def add_row(table, row_name, latencies):
    table.append_row([row_name, '{:.2f}'.format(min(latencies)),
        '{:.2f}'.format(np.percentile(latencies, 50)),
        '{:.2f}'.format(np.percentile(latencies, 90)),
        '{:.2f}'.format(np.percentile(latencies, 99)),
        '{:.2f}'.format(np.percentile(latencies, 99.9)),
        '{:.2f}'.format(max(latencies))])
    return

def main(ack_latencies_file, store_latencies_file, load_latencies_file):
    table = BeautifulTable()
    table.column_headers = ['', 'min [us]', 'median [us]',
            'p90 [us]', 'p99 [us]', 'p99.9 [us]', 'max [us]']

    if ack_latencies_file is not None:
        ack_latencies = np.loadtxt(ack_latencies_file, delimiter='\n')
        add_row(table, 'ACK', ack_latencies)

    if store_latencies_file is not None:
        store_latencies = np.loadtxt(store_latencies_file, delimiter=' ')
        store_latencies = store_latencies[store_latencies != 0]
        store_latencies = np.true_divide(store_latencies, 1000.0)
        add_row(table, 'store', store_latencies)

    if load_latencies_file is not None:
        load_latencies = np.loadtxt(load_latencies_file, delimiter=' ')
        load_latencies = load_latencies[load_latencies != 0]
        load_latencies = np.true_divide(load_latencies, 1000.0)
        add_row(table, 'load', load_latencies)

    print(table)

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--ack-latencies-file',
            help='file with ACK latencies', required=False)
    parser.add_argument('--store-latencies-file',
            help='file with store latencies', required=False)
    parser.add_argument('--load-latencies-file',
            help='file with load latencies', required=False)
    args = parser.parse_args()
    main(args.ack_latencies_file, args.store_latencies_file,
            args.load_latencies_file)
