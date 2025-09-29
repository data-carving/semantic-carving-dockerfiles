# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

ARG BASE_IMAGE=ubuntu:22.04
FROM $BASE_IMAGE

LABEL maintainer="raffay.atiq@sri.com"
LABEL build_date="04-23-2024"

ENV TZ=US \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /Feature_Database

# Installing package dependencies
RUN apt-get update && \
	apt-get upgrade -y && \
	apt-get install -y git wget bzip2 libtool m4 automake build-essential python3 python3-pip python3-dev python3-venv libblas-dev liblapack-dev libatlas-base-dev gfortran libopenblas-dev libblis-dev libmkl-dev curl openssl libssl-dev libcurl4-openssl-dev libgeos-dev python3-tk libhdf4-dev libproj-dev proj-data proj-bin libxml2-dev
RUN wget https://ftp.gnu.org/gnu/autoconf/autoconf-2.71.tar.xz && \
	tar -xf autoconf-2.71.tar.xz && \
	cd autoconf-2.71/ && \
	chmod -R 777 ./ && \
	./configure && \
	make && \
	make install

# Setting up Python virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV HDF5_CARVE_LIBRARY=/usr/local/hdf5
ENV ZDIR=/usr/local
ENV NCDIR=/usr/local
ENV LD_PRELOAD_CARVE_COMMAND="./hdf5-selections-carving/h5carve.so /usr/local/hdf5/lib/libhdf5.so /usr/local/lib/libnetcdf.so"
# ENV NETCDF4=true
ENV HDF5_VERSION=1.14.6

# Installing Python dependencies
RUN python3 -m pip install connected-components-3d netCDF4 pandas starepandas dask[dataframe] numpy geopandas

# Creating directory to store pickle files
RUN mkdir pickles

# Installing zlib library
RUN wget https://www.zlib.net/zlib-1.3.1.tar.gz && \
    tar -xzvf zlib-1.3.1.tar.gz && \
    cd zlib-1.3.1 && \
    ./configure --prefix=${ZDIR} && \
    make check && \
    make install

# Installing HDF5 library
RUN wget https://github.com/HDFGroup/hdf5/releases/download/hdf5_$HDF5_VERSION/hdf5-$HDF5_VERSION.tar.gz && \
    tar -xvzf hdf5-$HDF5_VERSION.tar.gz && \
    cd hdf5-$HDF5_VERSION && \
    libtoolize --force && \
    aclocal && \
    autoheader && \
    automake --force-missing --add-missing && \
    autoconf && \
    ./configure --prefix=$HDF5_CARVE_LIBRARY --enable-ros3-vfd --with-zlib=${ZDIR} --enable-hl && \
    make && \
    make install && \
    make check-install && \
    HDF5_DIR=$HDF5_CARVE_LIBRARY pip3 install --no-binary=h5py git+https://github.com/h5py/h5py.git@master
    
# Installing netCDF4
RUN git clone https://github.com/Unidata/netcdf-c.git && \
    cd netcdf-c && \
    libtoolize --force && \
    aclocal && \
    autoheader && \
    automake --force-missing --add-missing && \
    autoconf && \
    CPPFLAGS='-I/usr/local/hdf5/include -I/usr/local/include' LDFLAGS='-L/usr/local/hdf5/lib -L/usr/local/lib' ./configure --disable-dap --disable-nczarr && \
    make check && \
    make install && \
    pip3 install netCDF4

RUN git clone -b experimental https://raffayatiq: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@github.com/data-carving/hdf5-selections-carving.git && \
    cd hdf5-selections-carving && \
    HDF5_CFLAGS="-fPIC" /usr/local/hdf5/bin/h5cc -shlib -shared H5carve_helper_functions.c H5selections_carve.c -o h5carve.so


# Copying script files
COPY ./featureDB_make_labels.py ./featureDB_make_labels.py
COPY ./featureDB_make_labels_h5py.py ./featureDB_make_labels_h5py.py
COPY ./featureDB_make_labels_h5py_re-execution.py ./featureDB_make_labels_h5py_re-execution.py
COPY ./featureDB_make_labels_dask_original.py ./featureDB_make_labels_dask_original.py
COPY ./featureDB_make_labels_dask_execution.py ./featureDB_make_labels_dask_execution.py
COPY ./featureDB_make_labels_dask_re-execution.py ./featureDB_make_labels_dask_re-execution.py

COPY ./interposed_dask_array.py ./interposed_dask_array.py

COPY ./check_pickle_files.py ./check_pickle_files.py
RUN pip3 install geopandas==0.14.4

RUN apt-get update && \
	apt-get install nano

COPY get_carving_results_hdf5.sh get_carving_results_hdf5.sh
COPY get_carving_results_dask.sh get_carving_results_dask.sh

RUN chmod a+x get_carving_results_hdf5.sh
RUN chmod a+x get_carving_results_dask.sh

RUN apt-get install bc

COPY rechunk.sh rechunk.sh

RUN apt-get install -y libhdf5-dev hdf5-helpers hdf5-tools

COPY run_hdf5.sh run_hdf5.sh
COPY run_dask.sh run_dask.sh

RUN chmod a+x run_hdf5.sh
RUN chmod a+x run_dask.sh

COPY run.sh run.sh
RUN chmod a+x run.sh
