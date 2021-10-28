import matplotlib.pyplot as plt
import numpy as np
import math
from beautifultable import BeautifulTable

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



latencies = np.loadtxt("store_measure_ns", delimiter=' ')
latencies = latencies[latencies != 0]
latencies = np.true_divide(latencies, 1000.0)
#bins = np.histogram_bin_edges(latencies, bins='auto')

bins = np.linspace(math.ceil(min(latencies)),
                   math.floor(max(latencies)),
                   20)

outlier_aware_hist(latencies, 0, 15)

#plt.xlim([min(latencies)-5, max(latencies)+5])
#plt.hist(latencies, bins=bins, alpha=0.5)
#plt.show()

table = BeautifulTable()
table.column_headers = ['', 'min [us]', 'median [us]',
        'p90 [us]', 'p99 [us]', 'p99.9 [us]', 'max [us]']


load_latencies = np.loadtxt("load_times.txt", delimiter=' ')
load_latencies = load_latencies[load_latencies != 0]
load_latencies = np.true_divide(load_latencies, 1000.0)

table.append_row(['load', '{:.2f}'.format(min(load_latencies)),
    '{:.2f}'.format(np.percentile(load_latencies, 50)),
    '{:.2f}'.format(np.percentile(load_latencies, 90)),
    '{:.2f}'.format(np.percentile(load_latencies, 99)),
    '{:.2f}'.format(np.percentile(load_latencies, 99.9)),
    '{:.2f}'.format(max(load_latencies))])

table.append_row(['store', '{:.2f}'.format(min(latencies)),
    '{:.2f}'.format(np.percentile(latencies, 50)),
    '{:.2f}'.format(np.percentile(latencies, 90)),
    '{:.2f}'.format(np.percentile(latencies, 99)),
    '{:.2f}'.format(np.percentile(latencies, 99.9)),
    '{:.2f}'.format(max(latencies))])

ack_latencies = np.loadtxt("times.txt", delimiter='\n')
table.append_row(['store ACK', '{:.2f}'.format(min(ack_latencies)),
    '{:.2f}'.format(np.percentile(ack_latencies, 50)),
    '{:.2f}'.format(np.percentile(ack_latencies, 90)),
    '{:.2f}'.format(np.percentile(ack_latencies, 99)),
    '{:.2f}'.format(np.percentile(ack_latencies, 99.9)),
    '{:.2f}'.format(max(ack_latencies))])

print(table)

#print(bins)
#print(latencies)
