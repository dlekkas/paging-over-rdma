import datetime
import numpy as np
from sklearn.cluster import KMeans
from sklearn.datasets import make_blobs

start = datetime.datetime.now()

np.random.seed(42)
samples, labels = make_blobs(n_samples=2000000, centers=10, random_state=0)
#print(samples[:50])
k_means = KMeans(10)
k_means.fit(samples)

end = datetime.datetime.now()
elapsed = end - start
print(int(elapsed.total_seconds() * 1000))
