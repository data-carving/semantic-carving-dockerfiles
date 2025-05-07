import cc3d
import netCDF4
import glob
import numpy
import sys
import datetime
import pickle
import time
import pandas
import h5py
import ast
import sys
import interposed_dask_array as da
import numpy as np

dask_array_chunkshape = ast.literal_eval(sys.argv[1])
chunkshape_input = sys.argv[1].strip("()").replace(" ", "").replace(",", "_")

#
file_paths = sorted(glob.glob(f'/shared/dask_chunks_{chunkshape_input}' + '/imerg2022/3B-HHR*.HDF5'))
# file_paths = sorted(glob.glob('.' + '/3B-HHR*selection'))


# Selection Part
data_to_pickle = []
data = []
filenames = []
headers = []
timestamps = []

for file_path in file_paths:
	f = h5py.File(file_path, 'r')
	
	data_object = f['Grid']['precipitationCal']

	data_to_pickle.append(data_object[()][0].T[::-1])
	data.append(data_object)
	filenames.append(f.filename)

	file_header = f['/'].attrs['FileHeader'].decode("utf-8").split(';\n')
	file_header.remove('')
	header = {r.split('=')[0]: r.split('=')[1] for r in file_header}
	headers.append(header)
	timestamps.append(header['StartGranuleDateTime'])

timestamps_dt = [datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.000Z') for date in timestamps]
timestamps_np = numpy.array(timestamps_dt, dtype='datetime64[m]')

with open('./pickles/data.pickle', 'wb') as f:
    pickle.dump(np.array(data_to_pickle), f)

with open('./pickles/timestamps.pickle', 'wb') as f:
    pickle.dump(timestamps_dt, f)
# data = imerg

thresh = 0.5

for i in range(0, len(data)):
	datapath = filenames[i] + '+/Grid/precipitationCal'
	dask_data = da.from_array(data[i][()], chunks=dask_array_chunkshape, name=datapath)
	c = dask_data >= thresh
	c.datapath = datapath
	mask = c.compute()
	da.output_carved(delete=True)

	data[i] = mask[0].T[::-1]

data = numpy.array(data)
connectivity = 6 
min_voxels = 1000

start = time.time()
labels, N = cc3d.connected_components(data, delta=0,connectivity=connectivity, return_N=True)
print(time.time() - start)

start = time.time()
cc3d.dust(labels, threshold=min_voxels, connectivity=connectivity, in_place=True)
print(time.time() - start)

label_names = numpy.unique(labels[labels>0])
label_names.shape

start = time.time()
largest_20, N = cc3d.largest_k(labels, k=20, connectivity=connectivity, delta=0, return_N=True)
largest_100, N = cc3d.largest_k(labels, k=100, connectivity=connectivity, delta=0, return_N=True)
print(time.time() - start)

with open('./pickles/largest_20.pickle', 'wb') as f:
    pickle.dump(largest_20, f)
    
with open('./pickles/largest_100.pickle', 'wb') as f:
    pickle.dump(largest_100, f)

with open('./pickles/labels.pickle', 'wb') as f:
    pickle.dump(labels, f)


# data = f['Grid']['precipitationCal']

# # print(data.shape)

# thresh = 0.5
# mask = data[()] >= thresh

# print(np.where(mask))
# print(data[mask])