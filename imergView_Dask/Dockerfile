# Start from a Miniconda base image
# FROM continuumio/miniconda3
FROM ubuntu:22.04

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git wget bzip2 libtool m4 automake build-essential python3-pip python3-dev python3-venv curl openssl libssl-dev libcurl4-openssl-dev libxml2-dev bc

# Install conda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && \
    chmod +x miniconda.sh && \
    ./miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh && \
    /opt/conda/bin/conda init bash

# Set the PATH environment variable to include conda
ENV PATH=/opt/conda/bin:$PATH

# Install the necessary Conda packages from conda-forge
RUN conda install -y -c conda-forge \
    cartopy \
    gdal \
    geopandas \
    matplotlib \
    netcdf4 \
    numpy \
    pandas \
    proj \
    pyproj \
    pyshp \
    shapely \
    tqdm \
    dask>=2024.5.0 \
    distributed>=2024.6.2 \
    cloudpickle>=3.1.0 && \
    conda clean -afy  # Clean up Conda cache to reduce image size

# RUN conda install -y -c conda-forge connected-components-3d

# Install additional packages to resolve libarchive.so.20 issue
RUN conda install --solver=classic -y -c conda-forge \
    conda-libmamba-solver \
    libmamba \
    libmambapy \
    libarchive && \
    conda clean -afy

# Update the package manager and install dependencies for FlexFS
RUN apt-get update && \
    apt-get install -y fuse curl && \
    rm -rf /var/lib/apt/lists/*

# Install FlexFS
# RUN curl -OJ https://get.flexfs.io/v1.4.x/linux/$(uname -m)/mount.flexfs && \
#    chmod +x mount.flexfs && \
#    mv mount.flexfs /sbin

# Set FUSE configuration to allow non-root users
RUN echo "user_allow_other" >> /etc/fuse.conf

# Make the imergview folder
RUN mkdir -p /imergview

# Copy the startup script and make it executable
# COPY start-flexfs.sh /usr/local/bin/start-flexfs.sh
# RUN chmod +x /usr/local/bin/start-flexfs.sh

# Set the entrypoint to the startup script
# ENTRYPOINT ["/usr/local/bin/start-flexfs.sh"]

RUN conda install -y -c conda-forge gxx_linux-64

RUN conda uninstall netcdf4

RUN wget https://ftp.gnu.org/gnu/autoconf/autoconf-2.72.tar.xz && \
    tar -xf autoconf-2.72.tar.xz && \
    cd autoconf-2.72/ && \
    chmod -R 777 ./ && \
    ./configure && \
    make && \
    make install

# Setting up Python virtual environment
# ENV VIRTUAL_ENV=/opt/venv
# RUN python3 -m venv $VIRTUAL_ENV
# ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV HDF5_CARVE_LIBRARY=/usr/local/hdf5
ENV ZDIR=/usr/local
ENV NCDIR=/usr/local
ENV LD_PRELOAD_CARVE_COMMAND="../hdf5-selections-carving/h5carve.so /usr/local/hdf5/lib/libhdf5.so /usr/local/lib/libnetcdf.so"
ENV NETCDF4=true
ENV HDF5_VERSION=1.14.6

RUN pip install connected-components-3d

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

RUN conda uninstall hdf5

COPY interposed_dask_array.py /imergview/interposed_dask_array.py
COPY pf_search_dask_original.py /imergview/pf_search_dask_original.py
COPY pf_search_dask_execution.py /imergview/pf_search_dask_execution.py
COPY pf_search_dask_re-execution.py /imergview/pf_search_dask_re-execution.py
COPY pf_search_h5py.py /imergview/pf_search_h5py.py
COPY pf_search_h5py_re-execution.py /imergview/pf_search_h5py_re-execution.py

COPY get_carving_results_hdf5.sh /imergview/get_carving_results_hdf5.sh

COPY dask_test.py /imergview/dask_test.py
COPY selections_test.py /imergview/selections_test.py

RUN chmod a+x /imergview/get_carving_results_hdf5.sh

COPY run_hdf5.sh /imergview/run_hdf5.sh
RUN chmod a+x /imergview/run_hdf5.sh

COPY run_dask.sh /imergview/run_dask.sh
RUN chmod a+x /imergview/run_dask.sh

COPY rechunk.sh /imergview/rechunk.sh
RUN chmod a+x /imergview/rechunk.sh

RUN apt-get update && apt-get install -y libhdf5-dev hdf5-helpers hdf5-tools

COPY get_carving_results_dask.sh /imergview/get_carving_results_dask.sh
RUN chmod a+x /imergview/get_carving_results_dask.sh

COPY run.sh /imergview/run.sh
RUN chmod a+x /imergview/run.sh

COPY check_npy_files.py /imergview/check_npy_files.py

WORKDIR /imergview

# Run an interactive shell by default
# CMD ["/bin/bash"]