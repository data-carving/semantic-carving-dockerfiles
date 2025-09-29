# Copyright (c) 2025 SRI International All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

ARG BASE_IMAGE=ubuntu:22.04
FROM $BASE_IMAGE

LABEL maintainer="raffay.atiq@sri.com"
LABEL build_date="11-13-2024"

ENV TZ=US \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /POMD-PF

# Installing package dependencies
RUN apt-get update && \
	apt-get upgrade -y && \
	apt-get install -y git wget bzip2 libtool m4 automake build-essential python3-pip python3-dev python3-venv libblas-dev liblapack-dev libatlas-base-dev gfortran libopenblas-dev libblis-dev libmkl-dev libgeos-dev python3-tk libhdf4-dev libproj-dev proj-data proj-bin curl openssl libssl-dev libcurl4-openssl-dev libxml2-dev nano bc

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
RUN python3 -m pip install numpy pandas pyhdf matplotlib scipy cartopy

# Copying script and data files
#COPY ./POMD-PF.AIST.SA.Storms_h5py.py ./POMD-PF.AIST.SA.Storms_h5py.py
#COPY ./POMD-PF.AIST.SA.Storms_h5py_dask.py ./POMD-PF.AIST.SA.Storms_dask.py
#COPY ./POMD ./POMD

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

# RUN apt-get install -y libhdf5-dev hdf5-helpers hdf5-tools

RUN git clone -b experimental https://raffayatiq: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@github.com/data-carving/hdf5-selections-carving.git && \
    cd hdf5-selections-carving && \
    HDF5_CFLAGS="-fPIC" /usr/local/hdf5/bin/h5cc -shlib -shared H5carve_helper_functions.c H5selections_carve.c -o h5carve.so

RUN pip3 install dask && \
	pip3 install "dask[distributed]" --upgrade && \
	pip3 install pyarrow

COPY ./interposed_dask_dataframe.py ./interposed_dask_dataframe.py

COPY ./POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_original.py ./POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_original.py
COPY ./POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_execution.py ./POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_execution.py
COPY ./POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_re-execution.py ./POMD-PF.AIST.SA.Storms_h5py_dask_dataframe_re-execution.py
COPY ./POMD-PF.AIST.SA.Storms_h5py_selections.py ./POMD-PF.AIST.SA.Storms_h5py_selections.py
COPY ./POMD-PF.AIST.SA.Storms_h5py_selections_re-execution.py ./POMD-PF.AIST.SA.Storms_h5py_selections_re-execution.py

COPY ./get_carving_results.sh ./get_carving_results.sh
COPY ./original.sh ./original.sh
COPY ./execute.sh ./execute.sh
COPY ./re-execute.sh ./re-execute.sh
COPY ./reproduce_results.sh ./reproduce_results.sh

RUN chmod a+x ./*.sh

RUN pip3 install jupyter
