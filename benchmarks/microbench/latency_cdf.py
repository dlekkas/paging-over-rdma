import numpy as np
import matplotlib.pyplot as plt
from matplotlib import mlab

load_latencies = np.loadtxt("load_times.txt", delimiter=' ')
load_latencies = load_latencies[load_latencies != 0]
load_latencies = np.true_divide(load_latencies, 1000.0)
print(load_latencies)

load_latencies = np.sort(load_latencies)
counts, bin_edges = np.histogram(load_latencies, bins=50)
pdf = counts / sum(counts)
cdf = np.cumsum(pdf)
plt.plot(bin_edges[1:], cdf)
plt.ylim(0.998, 1)
plt.xlim([0,100])
plt.grid(True)
plt.ylabel('Percentile')
plt.xlabel('Pagein latency (us)')
plt.title('Latency CDF')
plt.show()
