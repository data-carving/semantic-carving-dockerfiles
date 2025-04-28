#! /usr/bin/env python -tt
# -*- coding: utf-8; mode: python -*-
r"""

pf_search
~~~~~~~~~
"""
# Standard Imports
import os
import typing
from datetime import datetime
import pickle
import math
from multiprocessing import Pool

# Third-Party Imports
import numpy as np
import numpy.ma as ma
import numpy.typing as npt
# import netCDF4
import h5py
import cc3d
from tqdm import tqdm
# import pandas
# import geopandas
import matplotlib.pyplot as plt
import matplotlib as mpl
import cartopy
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cycler

# STARE Imports
# import pystare

# Local Imports

##
# List of Public objects from this module.
__all__ = ['pf_search']

##
# Markup Language Specification (see NumpyDoc Python Style Guide https://numpydoc.readthedocs.io/en/latest/format.html)
__docformat__ = "Numpydoc"
# ------------------------------------------------------------------------------

##
# Define Constants and State Variables
# ------------------------------------
def LON_TO_180(x): return ((x + 180.0) % 360.0) - 180.0
def LON_TO_360(x): return (x + 360.0) % 360.0

##
# Type Aliases
Optin: typing.TypeAlias = typing.Optional[typing.Union[bool, None]]

###############################################################################
# PUBLIC debug_print()
# --------------------
def debug_print(ar_map: npt.ArrayLike, a_time_str: str, tidx: int, lons: npt.ArrayLike, lats: npt.ArrayLike, dl_lon_idx: int) -> None:
    """Print debug information

    Parameters
    ----------
    ar_map : npt.ArrayLike
        _description_
    a_time_str : str
        _description_
    pname : str
        _description_
    tidx : int
        _description_
    lons : npt.ArrayLike
        _description_
    lats : npt.ArrayLike
        _description_
    dl_lon_idx : int
        _description_

    """
    nlons = len(lons)
    nlats = len(lats)
    _tmp1 = "".join([("^" if _ in [0, dl_lon_idx, nlons - 1] else " ") for _ in range(nlons)])
    _tmp2 = f"0{_tmp1[1:]}"
    _tmp2 = f"{_tmp2[:-1]}575"
    _tmp1 = f"0.0{_tmp1[1:]}"
    _tmp1 = f"{_tmp1[:-6]}-0.625"
    _tmp1 = _tmp1.replace("     ^", "180.0")
    msg = []
    msg.append(f'Time Index {tidx:3d} {a_time_str.replace("T", "-")} UTC\n')
    for jidx in range(nlats):
        _tmp = "".join([("*" if _ else "-") for _ in ar_map[jidx, :]])
        msg.append(f"{tidx:3d} {jidx:3d} {lats[jidx]:+8.3f}: {_tmp} ")
    msg.append(f"                : {_tmp2}")
    msg.append(f"                : {_tmp1}\n")
    print('\n'.join(msg))

    return

###############################################################################
# PUBLIC basic_plot_pool()
# ------------------------
def basic_plot_pool(map_this: str, lons: npt.ArrayLike, lats: npt.ArrayLike, the_time: str, pname: str) -> str:
    """Create a basic plot

    Parameters
    ----------
    map_this : npt.ArrayLike
        _description_
    lons : npt.ArrayLike
        _description_
    lats : npt.ArrayLike
        _description_
    the_time : str
        _description_
    pname : str
        _description_
    """

    ccl_map = np.load(map_this)

    #  122 colors
    all_colors = ['rosybrown', 'lightcoral', 'indianred', 'brown', 'firebrick', 'maroon', 'darkred', 'red',
                  'salmon', 'tomato', 'darksalmon', 'coral', 'orangered', 'lightsalmon', 'sienna',
                  'chocolate', 'saddlebrown', 'sandybrown', 'peachpuff', 'peru', 'linen', 'bisque',
                  'darkorange', 'burlywood', 'tan', 'navajowhite', 'blanchedalmond', 'papayawhip', 'moccasin',
                  'orange', 'wheat', 'oldlace', 'darkgoldenrod', 'goldenrod', 'cornsilk', 'gold', 'lemonchiffon',
                  'khaki', 'palegoldenrod', 'darkkhaki', 'olive',
                  'yellow', 'olivedrab', 'yellowgreen', 'darkolivegreen', 'greenyellow', 'chartreuse', 'lawngreen',
                  'darkseagreen', 'palegreen', 'lightgreen', 'forestgreen', 'limegreen', 'darkgreen',
                  'green', 'lime', 'seagreen', 'mediumseagreen', 'springgreen', 'mediumspringgreen',
                  'mediumaquamarine', 'aquamarine', 'turquoise', 'lightseagreen', 'mediumturquoise', 'lightcyan',
                  'paleturquoise', 'teal', 'darkcyan', 'aqua', 'cyan', 'darkturquoise',
                  'cadetblue', 'powderblue', 'lightblue', 'deepskyblue', 'skyblue', 'lightskyblue', 'steelblue',
                  'dodgerblue', 'lightsteelblue', 'cornflowerblue', 'royalblue', 'lavender', 'midnightblue', 'navy',
                  'darkblue', 'mediumblue', 'blue', 'slateblue', 'darkslateblue', 'mediumslateblue', 'mediumpurple',
                  'rebeccapurple', 'blueviolet', 'indigo', 'darkorchid', 'darkviolet', 'mediumorchid', 'thistle',
                  'plum', 'violet', 'purple', 'darkmagenta', 'fuchsia', 'magenta', 'orchid', 'mediumvioletred',
                  'deeppink', 'hotpink', 'lavenderblush', 'palevioletred', 'crimson', 'pink', 'lightpink']
    ncolors = len(all_colors)

    # # mpl.rcParams['axes.color_cycle'] = all_colors
    # n = 100
    # color = pyplot.cm.viridis(np.linspace(0, 1,n))
    # mpl.rcParams['axes.prop_cycle'] = cycler.cycler('color', color)

    # matplotlib.colors.ListedColormap
    # cmap = plt.cm.tab20b                          # get a specific colormap
    # cmaplist = cmap.colors                        # extract all colors
    # mm = 20 # he number of colors in your base colormap.
    # nn = 100

    # LinearSegmentedColormap
    cmap = plt.cm.gist_rainbow
    cmaplist = [mpl.colors.rgb2hex(cmap(i)) for i in range(cmap.N)]
    mm = 256
    nn = 100
    xx = math.ceil(nn / mm)
    cmaplistext = cmaplist * xx  # repeat X times, here X = 5
    customMap = mpl.colors.LinearSegmentedColormap.from_list('Custom cmap', cmaplistext, nn)
    # customMap = plt.cm.tab20b

    plot_dpi =  300
    globe = ccrs.Globe(datum='WGS84', ellipse='WGS84')

    lon_0_global = 0
    min_lat, max_lat = -90, 90
    min_lon, max_lon = -180, 180

    map_extent = (min_lon, max_lon, min_lat, max_lat)
    # print(map_extent)
    geod_crs = ccrs.Geodetic(globe=globe)
    flat_crs = ccrs.PlateCarree(central_longitude=0, globe=globe)
    map_crs = ccrs.PlateCarree(central_longitude=lon_0_global, globe=globe)
    if lon_0_global == 180:
        data_crs = flat_crs
    else:
        data_crs = map_crs

    fig = plt.figure(figsize=(12, 4), frameon=True)
    if pname.find("ccl") != -1:
        geo_axes = plt.axes(projection=map_crs, facecolor='k')
    else:
        geo_axes = plt.axes(projection=map_crs)

    # geo_axes.set_xlim(left=LON_TO_360(min_lon), right=LON_TO_360(max_lon))
    # geo_axes.set_ylim(bottom=min_lat, top=max_lat)
    if pname.find("ccl") != -1:
        geo_axes.add_feature(cfeature.COASTLINE, edgecolor='w', linewidth=0.25)
    else:
        geo_axes.add_feature(cfeature.COASTLINE)

    # geo_axes.contourf(lons, lats_nh, ma.masked_equal(map_this, 0), 1, transform=ccrs.PlateCarree())
    i_ways = ['none', 'antialiased', 'nearest', 'bilinear', 'bicubic', 'spline16', 'spline36',
              'hanning', 'hamming', 'hermite', 'kaiser', 'quadric', 'catrom', 'gaussian', 'bessel',
              'mitchell', 'sinc', 'lanczos', 'blackman']
    iway = i_ways[1]
    if pname.find("ccl") != -1:
        # param_dict = {"extent": map_extent, "interpolation": iway, "cmap": "tab20", "origin": 'upper'}
        param_dict = {"extent": map_extent, "interpolation": iway, "cmap": customMap, "origin": 'lower'}
    else:
        param_dict = {"extent": map_extent, "interpolation": iway, "cmap": "cool", "vmin": 0, "vmax": 1, "origin": 'lower'}
    geo_axes.imshow(ma.masked_equal(ccl_map, 0), transform=map_crs, **param_dict)

    # now_ccl = sorted(np.unique(ccl_map[ccl_map > 0]).tolist())
    # n_ccl = len(now_ccl)
    # # print(f"now_ccl  ({n_ccl:4d}): {now_ccl}\n\n")
    # grid_lons, grid_lats = np.meshgrid(lons, lats)
    # grid_lons = grid_lons.flatten()
    # grid_lats = grid_lats.flatten()
    # for nl in now_ccl:
    #     nl_mask = np.where(ccl_map == nl, 1, 0)
    #     nl_mask = nl_mask.flatten()
    #     nl_mask_hits = np.nonzero(nl_mask)[0]
    #     now_labs_grids = nl_mask_hits.tolist()
    #     nl_lons = grid_lons[now_labs_grids]
    #     nl_lats = grid_lats[now_labs_grids]
    #     nl_mean_lon = np.mean(nl_lons)
    #     nl_mean_lat = np.mean(nl_lats)
    #     geo_axes.text(nl_mean_lon, nl_mean_lat, f"{nl}", color='w', fontsize=3, weight='bold', horizontalalignment='center', transform=geod_crs)

    tcolor = "w" if pname.find("ccl") != -1 else "k"
    geo_axes.text(0, 80, the_time, color=tcolor, fontsize=10, weight='bold', horizontalalignment='center', transform=geod_crs)

    fig.savefig(pname, dpi=plot_dpi, facecolor='w', edgecolor='w',
                orientation='landscape', bbox_inches='tight', pad_inches=0.02)
    plt.clf()
    plt.close('all')

    return pname

###############################################################################
# PUBLIC basic_plot()
# -------------------
def basic_plot(map_this: npt.ArrayLike, lons: npt.ArrayLike, lats: npt.ArrayLike, the_time: str, pname: str) -> None:
    """Create a basic plot

    Parameters
    ----------
    map_this : npt.ArrayLike
        _description_
    lons : npt.ArrayLike
        _description_
    lats : npt.ArrayLike
        _description_
    the_time : str
        _description_
    pname : str
        _description_
    """

    #  122 colors
    all_colors = ['rosybrown', 'lightcoral', 'indianred', 'brown', 'firebrick', 'maroon', 'darkred', 'red',
                  'salmon', 'tomato', 'darksalmon', 'coral', 'orangered', 'lightsalmon', 'sienna',
                  'chocolate', 'saddlebrown', 'sandybrown', 'peachpuff', 'peru', 'linen', 'bisque',
                  'darkorange', 'burlywood', 'tan', 'navajowhite', 'blanchedalmond', 'papayawhip', 'moccasin',
                  'orange', 'wheat', 'oldlace', 'darkgoldenrod', 'goldenrod', 'cornsilk', 'gold', 'lemonchiffon',
                  'khaki', 'palegoldenrod', 'darkkhaki', 'olive',
                  'yellow', 'olivedrab', 'yellowgreen', 'darkolivegreen', 'greenyellow', 'chartreuse', 'lawngreen',
                  'darkseagreen', 'palegreen', 'lightgreen', 'forestgreen', 'limegreen', 'darkgreen',
                  'green', 'lime', 'seagreen', 'mediumseagreen', 'springgreen', 'mediumspringgreen',
                  'mediumaquamarine', 'aquamarine', 'turquoise', 'lightseagreen', 'mediumturquoise', 'lightcyan',
                  'paleturquoise', 'teal', 'darkcyan', 'aqua', 'cyan', 'darkturquoise',
                  'cadetblue', 'powderblue', 'lightblue', 'deepskyblue', 'skyblue', 'lightskyblue', 'steelblue',
                  'dodgerblue', 'lightsteelblue', 'cornflowerblue', 'royalblue', 'lavender', 'midnightblue', 'navy',
                  'darkblue', 'mediumblue', 'blue', 'slateblue', 'darkslateblue', 'mediumslateblue', 'mediumpurple',
                  'rebeccapurple', 'blueviolet', 'indigo', 'darkorchid', 'darkviolet', 'mediumorchid', 'thistle',
                  'plum', 'violet', 'purple', 'darkmagenta', 'fuchsia', 'magenta', 'orchid', 'mediumvioletred',
                  'deeppink', 'hotpink', 'lavenderblush', 'palevioletred', 'crimson', 'pink', 'lightpink']
    ncolors = len(all_colors)

    # # mpl.rcParams['axes.color_cycle'] = all_colors
    # n = 100
    # color = pyplot.cm.viridis(np.linspace(0, 1,n))
    # mpl.rcParams['axes.prop_cycle'] = cycler.cycler('color', color)

    # matplotlib.colors.ListedColormap
    # cmap = plt.cm.tab20b                          # get a specific colormap
    # cmaplist = cmap.colors                        # extract all colors
    # mm = 20 # he number of colors in your base colormap.
    # nn = 100

    # LinearSegmentedColormap
    cmap = plt.cm.gist_rainbow
    cmaplist = [mpl.colors.rgb2hex(cmap(i)) for i in range(cmap.N)]
    mm = 256
    nn = 100
    xx = math.ceil(nn / mm)
    cmaplistext = cmaplist * xx  # repeat X times, here X = 5
    customMap = mpl.colors.LinearSegmentedColormap.from_list('Custom cmap', cmaplistext, nn)
    # customMap = plt.cm.tab20b

    plot_dpi =  300
    globe = ccrs.Globe(datum='WGS84', ellipse='WGS84')

    lon_0_global = 0
    min_lat, max_lat = -90, 90
    min_lon, max_lon = -180, 180

    map_extent = (min_lon, max_lon, min_lat, max_lat)
    # print(map_extent)
    geod_crs = ccrs.Geodetic(globe=globe)
    flat_crs = ccrs.PlateCarree(central_longitude=0, globe=globe)
    map_crs = ccrs.PlateCarree(central_longitude=lon_0_global, globe=globe)
    if lon_0_global == 180:
        data_crs = flat_crs
    else:
        data_crs = map_crs

    fig = plt.figure(figsize=(12, 4), frameon=True)
    if pname.find("ccl") != -1:
        geo_axes = plt.axes(projection=map_crs, facecolor='k')
    else:
        geo_axes = plt.axes(projection=map_crs)

    # geo_axes.set_xlim(left=LON_TO_360(min_lon), right=LON_TO_360(max_lon))
    # geo_axes.set_ylim(bottom=min_lat, top=max_lat)
    if pname.find("ccl") != -1:
        geo_axes.add_feature(cfeature.COASTLINE, edgecolor='w', linewidth=0.25)
    else:
        geo_axes.add_feature(cfeature.COASTLINE)

    # geo_axes.contourf(lons, lats_nh, ma.masked_equal(map_this, 0), 1, transform=ccrs.PlateCarree())
    i_ways = ['none', 'antialiased', 'nearest', 'bilinear', 'bicubic', 'spline16', 'spline36',
              'hanning', 'hamming', 'hermite', 'kaiser', 'quadric', 'catrom', 'gaussian', 'bessel',
              'mitchell', 'sinc', 'lanczos', 'blackman']
    iway = i_ways[1]
    if pname.find("ccl") != -1:
        # param_dict = {"extent": map_extent, "interpolation": iway, "cmap": "tab20", "origin": 'upper'}
        param_dict = {"extent": map_extent, "interpolation": iway, "cmap": customMap, "origin": 'lower'}
    else:
        param_dict = {"extent": map_extent, "interpolation": iway, "cmap": "cool", "vmin": 0, "vmax": 1, "origin": 'lower'}
    geo_axes.imshow(ma.masked_equal(map_this, 0), transform=map_crs, **param_dict)

    now_ccl = sorted(np.unique(map_this[map_this > 0]).tolist())
    n_ccl = len(now_ccl)
    # print(f"now_ccl  ({n_ccl:4d}): {now_ccl}\n\n")
    grid_lons, grid_lats = np.meshgrid(lons, lats)
    grid_lons = grid_lons.flatten()
    grid_lats = grid_lats.flatten()
    for nl in now_ccl:
        nl_mask = np.where(map_this == nl, 1, 0)
        nl_mask = nl_mask.flatten()
        nl_mask_hits = np.nonzero(nl_mask)[0]
        now_labs_grids = nl_mask_hits.tolist()
        nl_lons = grid_lons[now_labs_grids]
        nl_lats = grid_lats[now_labs_grids]
        nl_mean_lon = np.mean(nl_lons)
        nl_mean_lat = np.mean(nl_lats)
        geo_axes.text(nl_mean_lon, nl_mean_lat, f"{nl}", color='w', fontsize=3, weight='bold', horizontalalignment='center', transform=geod_crs)

    # mikemike
    # os._exit(0)

    tcolor = "w" if pname.find("ccl") != -1 else "k"
    geo_axes.text(0, 80, the_time, color=tcolor, fontsize=10, weight='bold', horizontalalignment='center', transform=geod_crs)

    fig.savefig(pname, dpi=plot_dpi, facecolor='w', edgecolor='w',
                orientation='landscape', bbox_inches='tight', pad_inches=0.02)
    plt.clf()
    plt.close('all')

    return

###############################################################################
# PUBLIC edge_check()
# -------------------
def edge_check(lmap) -> tuple[npt.ArrayLike, int]:
    """Check if ccl labels at end of map edges line up in latitude.

    Parameters
    ----------
    lmap : ndarray
        Result of applying cc3d.connected_components()

    Returns
    -------
    ndarray
        lmap possibly remapped.
    int
        Flag for if lmap remapped.
    """
    verbose = 0
    ##
    # Start map edge (-180 deg),  ndarray dtype=uint32)
    edge_0_labs = lmap[:, 0]
    # Note this will include CCL that cross and don't cross the map edge.
    is_edge_0 = np.unique(edge_0_labs[edge_0_labs > 0]).tolist()
    # if verbose:
    #     print(f"\tedge_check():\n\t\t{is_edge_0 = }")

    ##
    # End map edge (+180 deg)
    edge_1_labs = lmap[:, -1]
    is_edge_1 = np.unique(edge_1_labs[edge_1_labs > 0]).tolist()
    # if verbose:
    #     print(f"\tedge_check():\n\t\t{is_edge_1 = }")

    ##
    # Check if labels at both map edges line up in latitude.
    #   Assume features travel westerly (from West -> East or wrap from is_edge_1 -> is_edge_0)
    remapped_ccl = {}
    if is_edge_0 and is_edge_1:
        # if verbose:
        #     print(f"\n\t{'Idx':<4s} {'Edge0':<5s} {'Edge1':<5s}")
        #     for lidx in range(len(edge_1_labs)):
        #         if edge_0_labs[lidx] == 0 or edge_1_labs[lidx] == 0:
        #             continue
        #         print(f"\t{lidx:04d} {int(edge_0_labs[lidx]):5d} {int(edge_1_labs[lidx]):5d}")

        nlat = lmap.shape[0]

        # # Debug RAW
        # for jjidx in range(nlat):
        #     print(f"\t{jjidx:04d} {int(lmap[jjidx, -2]):4d} {int(lmap[jjidx, -1]):4d} | {int(lmap[jjidx, 0]):4d} {int(lmap[jjidx, 1]):4d}")
        # os._exit(1)

        # # Debug Focus
        # target_jidx = 1554
        # # print("\n\n")
        # # for jjidx in range(target_jidx - 1, target_jidx + 2, 1):
        # #     print(f"\t{jjidx:04d} {int(lmap[jjidx, -2]):4d} {int(lmap[jjidx, -1]):4d} | {int(lmap[jjidx, 0]):4d} {int(lmap[jjidx, 1]):4d}")
        # # print("\n\n")
        # # print()
        # # os._exit(1)

        for jidx in range(nlat):
            # print(f"Checking {jidx:04d}: {int(lmap[jidx, -2]):4d} {int(lmap[jidx, -1]):4d} | {int(lmap[jidx, 0]):4d} {int(lmap[jidx, 1]):4d}")
            ##
            # Guard against holes:
            """
            N/S Order
                0         SP
                          .
                          .
                          .
                nlat -1   NP

            E/W Order
                Edge_0                        Edge_1
                0       1   ...   nlon - 2    nlon - 1

            Moore Neighborhood around point 0
                1   2   3           1   2   3   jidx - 1
                8   0   4           8   0   4   jidx
                7   6   5           7   6   5   jidx + 1
                              iidx -1   0  +1

            Moore Neighborhood along Edge_0 not in a polar row
                edge_0_moore = [2, 4, 6]
                    here iidx == 0 means x index 0, '-' unused indices, '*' is index being tested
                            -  | 2   -   jidx - 1
                            -  | *   4   jidx
                            -  | 6   -   jidx + 1
                    iidx   -1  | 0  +1
                               ^
                               map edge

            Moore Neighborhood along Edge_1 not in a polar row
                edge_1_moore = [2, 6, 8]
                    here iidx == -1 means x index nlon - 1, '-' unused indices, '*' is index being tested
                            -  2 |  -   jidx - 1
                            8  * |  -   jidx
                            -  6 |  -   jidx + 1
                    iidx   -1  0 | +1
                                 ^
                                 map edge

            Moore Neighborhood at SPole
                edge_0_moore = [4, 6]
                            -  | *   4   jidx
                            -  | 6   -   jidx + 1
                    iidx   -1  | 0  +1
                               ^
                               map edge

                edge_1_moore = [6, 8]
                            8  * |  -   jidx
                            -  6 |  -   jidx + 1
                    iidx   -1  0 | +1
                                 ^
                                 map edge

            Moore Neighborhood at NPole
                edge_0_moore = [2, 4]
                            -  | 2   -   jidx - 1
                            -  | *   4   jidx
                    iidx   -1  | 0  +1
                               ^
                               map edge

                edge_1_moore = [2, 6]
                            -  2 |  -   jidx - 1
                            8  * |  -   jidx
                    iidx   -1  0 | +1
                                 ^
                                 map edge

            Here a 'hole' appears as a zero (no CCL label along a edge that has a neighbor that is labeled around it)

                A hole along Edge_0, here the CCL 46 has a hole at index 0 which without care might interfere with merging CCLs 46 and 44
                       44 | 46  46   jidx - 1
                       44 | 0   46   jidx
                       44 | 46  46   jidx + 1
                  iidx -1 | 0   +1
                          ^
                          map edge

                A hole along Edge_1, here the CCL 46 has a hole at index 0 which without care might interfere with merging CCLs 46 and 44
                       44  46 | 46   jidx - 1
                       44  0  | 46   jidx
                       44  46 | 46   jidx + 1
                 iidx  -1  0  | +1
                              ^
                              map edge
            """
            if jidx in [0, nlat - 1]:
                # Polar row
                if jidx == 0:
                    # At SPole
                    edge_0_moore = [lmap[jidx, 1], lmap[jidx + 1, 0]]
                    edge_1_moore = [lmap[jidx, -2], lmap[jidx + 1, -1]]
                else:
                    # At NPole
                    edge_0_moore = [lmap[jidx - 1, 0], lmap[jidx, 1]]
                    edge_1_moore = [lmap[jidx - 1, -1], lmap[jidx, -2]]
            else:
                edge_0_moore = [lmap[jidx - 1, 0], lmap[jidx, 1], lmap[jidx + 1, 0]]
                # 3 rows around edge_1 and jidx
                edge_1_moore = [lmap[jidx - 1, -1], lmap[jidx, -2], lmap[jidx + 1, -1]]

            # # Debug Edge
            # if jidx == target_jidx:
            #     print("\n\n")
            #     for jjidx in range(target_jidx - 1, target_jidx + 2, 1):
            #         print(f"\t{jjidx:04d} {int(lmap[jjidx, -2]):4d} {int(lmap[jjidx, -1]):4d} | {int(lmap[jjidx, 0]):4d} {int(lmap[jjidx, 1]):4d}")
            #     print("\n\n")
            #     print(f"{jidx:05d} {edge_0_moore = }\t{edge_0_labs[jidx] = }")
            #     print(f"{jidx:05d} {edge_1_moore = }\t{edge_1_labs[jidx] = }")
            #     os._exit(1)
            # # if jidx < target_jidx:
            # #     continue

            ##
            # Fill holes
            # print(f"{jidx:05d} {edge_0_moore = }\t{edge_0_labs[jidx] = }")
            if edge_0_labs[jidx] == 0 and len([_ for _ in edge_0_moore if _ != 0]) == 3:
                # Possible hole, find the largest neighboring CCL label in edge_0_moore and apply to edge_0_labs[jidx]
                edge_0_labs[jidx] = max(edge_0_moore)
            # print(f"{jidx:05d} {edge_1_moore = }\t{edge_1_labs[jidx] = }")
            if edge_1_labs[jidx] == 0 and len([_ for _ in edge_1_moore if _ != 0]) == 3:
                # Possible hole, find the largest CCL label in edge_1_moore and apply to edge_1_labs[jidx]
                edge_1_labs[jidx] = max(edge_1_moore)

            # # Debug Fill
            # if jidx == target_jidx:
            #     print(f"{jidx:05d} {edge_0_labs[jidx] = }")
            #     print(f"{jidx:05d} {edge_1_labs[jidx] = }")
            #     for ridx, rval in remapped_ccl.items():
            #         print(f"{ridx:4d}: {rval}")
            #     os._exit(1)
            # continue
            # if jidx < target_jidx:
            #     continue

            if edge_0_labs[jidx] > 0 and edge_1_labs[jidx] > 0:
                # CCL at same lat on both map edges
                # print(f"{jidx:05d} {edge_0_labs[jidx] = }\t{edge_1_labs[jidx] = }")
                if edge_1_labs[jidx] not in remapped_ccl:
                    # This CCL hasn't been remapped, so in in is_edge_1
                    if edge_0_labs[jidx] != edge_1_labs[jidx]:
                        # Remap CCL to is_edge_0 side
                        remapped_ccl[int(edge_1_labs[jidx])] = int(edge_0_labs[jidx])
                        if verbose:
                            print(f"\tRemapped {edge_1_labs[jidx]:4d} -> {edge_0_labs[jidx]:4d}")
                        # Replace all instances of remapped_ccl
                        lmap = np.where(lmap == int(edge_1_labs[jidx]), int(edge_0_labs[jidx]), lmap)
                        # if jidx == target_jidx:
                        #     print("\n\n")
                        #     for jjidx in range(target_jidx - 1, target_jidx + 2, 1):
                        #         print(f"\t{jjidx:04d} {int(lmap[jjidx, -2]):4d} {int(lmap[jjidx, -1]):4d} | {int(lmap[jjidx, 0]):4d} {int(lmap[jjidx, 1]):4d}")
                        #     print("\n\n")
                        #     os._exit(1)
            # # Debug
            # if jidx == target_jidx:
            #     os._exit(1)

            # Done lat loop
    return lmap, 1 if remapped_ccl else 0

###############################################################################
# PUBLIC track_labels()
# ---------------------
def track_labels(lmap_prev, lmap, live_ccl, dead_ccl, hemi_sep: tuple[int, int]) -> tuple[npt.ArrayLike, list[int], list[int], list[str]]:
    """Track CCL (Time Connect)

    Parameters
    ----------
    lmap_prev : ndarray
        CCL from previous time-step
    lmap : ndarray
        CCL from current time-step
    live_ccl : list
        List of active CCL
    dead_ccl : _type_
        List of inactive CCL

    Returns
    -------
    npt.ArrayLike
        Updated version of lmap
    list[int]
        Updated list of active CCL
    list[int]
        Updated list of inactive CCL
    """
    local_verbose = [False, True][0]
    local_verboser = [False, True][0]
    mega_msg = []
    if local_verbose:
        mega_msg.append("\tIn track_labels()")
    """
    Link (time connect or track) two fields of integer CCL labels
        lmap_prev : CCL from previous time-step
        lmap      : CCL from current time-step

        last_labs : list of unique CCL labels in lmap_prev
        now_labs  : list of unique CCL labels in lmap

        Issues to deal with:
            1) last_labs should be some sort of contiguous list of numbers from 1 to len(last_labs)-1
            2) now_labs should be some sort of contiguous list of numbers from 1 to len(now_labs)-1
            Thus, the CCL labels in last_labs have not relationship to those in now_labs but they can/will have similar/same values.
            To reduce this an offset of 5000 is applied to lmap and now_labs to keep them clearly separated.
            No offset is applied to last_labs as these labels include those held in live_ccl and dead_ccl.
    """
    ##
    # Set of CCL from previous time-step (not whole record!)
    last_labs = sorted(np.unique(lmap_prev[lmap_prev > 0]).tolist())
    last_labs_set = set(last_labs)
    if local_verbose:
        mega_msg.append(f"\t\tlast_labs ({len(last_labs)}): {last_labs}")

    ##
    # Set of current CCL, add offset so clear renaming
    lmap = np.where(lmap > 0, lmap + 5000, lmap)
    now_labs = sorted(np.unique(lmap[lmap > 0]).tolist())
    now_labs_set = set(now_labs)
    if local_verbose:
        mega_msg.append(f"\t\tnow_labs ({len(now_labs)}): {now_labs}")
        mega_msg.append("\t\tMapping now_labs to last_labs")
    # print('\n'.join(mega_msg)); return [], [], []

    """
    The first step loops over each member of now_labs (nl) and checks if that labels spatial grids directly correspond to the spatial grids of a member of last_labs (ll).
    If there is an overlap, we can assume that the now_labs label connects in time to the last_labs label.

        An entry is then made to the dictionary, if nl hasn't already been added to direct_overlap (see Issues below) a tuple is added
            direct_overlap[nl] = (ll, overlap_size)
        if nl is already in direct_overlap (i.e., nl overlaps with multiple members of last_labs), a list is created.
            direct_overlap[nl] = [(previous entries), ... (ll, overlap_size)]
        Issues to deal with:
            1) It is possible for the spatial grids of a last_labs label overlap with multiple spatial grids of now_labs labels.

    If no overlaps, no entry is made in direct_overlap, but nl does represent a newly started track.

    Example:

        Checking Now Lab        5001: nl_locs (7620)
            Checking Previous Lab      1: nl_locs (7620)        <= Simple overlap, entry in direct_overlap 5001: (1, 6785)
                Overlaps 6785

        ...

        Checking Now Lab        5008: nl_locs (1843)            <= Multiple overlaps, entry in direct_overlap 5008: [(11, 921), (19, 517)]
            Checking Previous Lab     11: nl_locs (1309)
                Overlaps 921
            Checking Previous Lab     19: nl_locs (645)
                Overlaps 517

        ...

        Checking Now Lab        5033: nl_locs (834)             <= No overlaps, new track, no entry in direct_overlap

    Speed ups
        Find now_labs and last_labs that are fully in NH or SH to limit searching.
        Likewise for E/W Hemisphere?

    """
    direct_overlap = {}

    ##
    # Find CCL that are wholly in one hemisphere or the other (speed up overlap checks)
    #   106 now_labs: 44 in NH, 54 in SH, and 8 span
    #   118 last_labs: 56 in NH, 54 in SH, and 8 span
    now_labs_nh = []
    now_labs_sh = []
    now_labs_grids = {}
    for nl in now_labs:
        ##
        # Check each current CCL
        nl_mask = np.where(lmap == nl, 1, 0)
        nl_mask = nl_mask.flatten()
        nl_mask_hits = np.nonzero(nl_mask)[0]
        now_labs_grids[nl] = set(nl_mask_hits.tolist())
        ##
        # CCL wholly in NH
        in_nh = True if np.amin(nl_mask_hits) >= hemi_sep[1] else False
        if in_nh:
            in_sh = False
            now_labs_nh.append(nl)
        else:
            ##
            # CCL wholly in SH
            in_sh = True if np.amax(nl_mask_hits) <= hemi_sep[0] else False
            if in_sh:
                now_labs_sh.append(nl)
    # for nl in now_labs_grids.keys():
    #     print(f"{nl:5d}: {len(now_labs_grids[nl])}")

    if local_verbose:
        tmp_ = len(now_labs_nh) + len(now_labs_sh)
        tmp__ = len(now_labs)
        mega_msg.append(f"\t\t{tmp__} now_labs: {len(now_labs_nh)} in NH, {len(now_labs_sh)} in SH, and {tmp__ - tmp_} span")
    last_labs_nh = []
    last_labs_sh = []
    last_labs_grids = {}
    for nl in last_labs:
        ##
        # Check each current CCL
        nl_mask = np.where(lmap_prev == nl, 1, 0)
        nl_mask = nl_mask.flatten()
        nl_mask_hits = np.nonzero(nl_mask)[0]
        last_labs_grids[nl] = set(nl_mask_hits.tolist())
        ##
        # CCL wholly in NH
        in_nh = True if np.amin(nl_mask_hits) >= hemi_sep[1] else False
        if in_nh:
            in_sh = False
            last_labs_nh.append(nl)
        else:
            ##
            # CCL wholly in SH
            in_sh = True if np.amax(nl_mask_hits) <= hemi_sep[0] else False
            if in_sh:
                last_labs_sh.append(nl)
    if local_verbose:
        tmp_ = len(last_labs_nh) + len(last_labs_sh)
        tmp__ = len(last_labs)
        mega_msg.append(f"\t\t{tmp__} last_labs: {len(last_labs_nh)} in NH, {len(last_labs_sh)} in SH, and {tmp__ - tmp_} span")
    # for nl in last_labs_grids.keys():
    #     print(f"{nl:5d}: {len(last_labs_grids[nl])}")

    now_labs_nh = set(now_labs_nh)
    now_labs_sh = set(now_labs_sh)
    last_labs_nh = set(last_labs_nh)
    last_labs_sh = set(last_labs_sh)
    for nl in now_labs:
        ##
        # Check each current CCL
        # nl_locs = np.where(lmap == nl)
        # if local_verboser:
        #     mega_msg.append(f"\t\tChecking Now Lab\t\t{nl:4d}: nl_locs ({len(nl_locs[0])})")

        # nl_mask = np.where(lmap == nl, 1, 0)
        # nl_mask = nl_mask.flatten()
        # nl_mask_hits = np.nonzero(nl_mask)[0]
        nl_mask_hits = now_labs_grids[nl]

        now_just_nh = True if nl in now_labs_nh else False
        now_just_sh = True if nl in now_labs_sh else False

        if local_verboser:
            mega_msg.append(f"\t\tChecking Now Lab\t\t{nl:4d}: nl_locs ({len(nl_mask_hits)}) {now_just_nh = } {now_just_sh = }")
        for ll in last_labs:
            last_just_nh = True if nl in last_labs_nh else False
            last_just_sh = True if nl in last_labs_sh else False
            if now_just_nh and last_just_sh:
                continue
            if now_just_sh and last_just_nh:
                continue
            ##
            # Check each previous time-step CCL for overlap
            # ll_mask = np.where(lmap_prev == ll, 1, 0)
            # ll_mask = ll_mask.flatten()
            # ll_mask_hits = np.nonzero(ll_mask)[0]
            ll_mask_hits = last_labs_grids[ll]
            # if local_verboser:
            #     mega_msg.append(f"\t\t\tChecking Previous Lab   {ll:4d}: nl_locs ({len(ll_mask_hits)})")

            # overlap = np.intersect1d(nl_mask_hits, ll_mask_hits)
            overlap = nl_mask_hits.intersection(ll_mask_hits)
            overlap_size = len(overlap)
            if overlap_size:
                if local_verboser:
                    mega_msg.append(f"\t\t\t\tOverlaps {overlap_size}")
                if nl in direct_overlap:
                    # Extend existing overlapping CCL entry
                    old = direct_overlap[nl]
                    if isinstance(old, tuple):
                        new = [old, (ll, overlap_size)]
                    else:
                        new.append((ll, overlap_size))
                    direct_overlap[nl] = new
                else:
                    # Create new overlapping CCL entry
                    direct_overlap[nl] = (ll, overlap_size)
            # break
        # break
    if local_verbose:
        mega_msg.append(f"\t\t{direct_overlap = }")
    # print('\n'.join(mega_msg))
    # os._exit(1)
    # print('\n'.join(mega_msg)); return [], [], []

    # # local_verboser =  True
    # direct_overlap = {5001: (1, 6785), 5002: (2, 1492), 5003: (5, 700), 5004: (5, 6430), 5005: (7, 684),
    #                   5006: (8, 5725), 5007: (3, 765), 5008: [(11, 921), (19, 517)], 5009: (9, 4161),
    #                   5010: (10, 2229), 5011: (12, 3242), 5012: (14, 471), 5013: (9, 2710), 5014: (13, 3277),
    #                   5015: (15, 1649), 5016: (9, 7047), 5017: [(16, 14283), (24, 682)], 5018: (17, 8777),
    #                   5019: (18, 907), 5020: (20, 327), 5021: (21, 959), 5022: (22, 14030), 5023: (23, 2916),
    #                   5024: (18, 510), 5025: (26, 782), 5026: (25, 1718), 5027: (27, 470), 5028: (28, 448),
    #                   5029: (16, 827), 5030: (29, 702), 5031: (30, 1325), 5032: (31, 2615), 5034: (32, 938),
    #                   5035: (33, 1221), 5037: (34, 818), 5038: (37, 869), 5039: (36, 1191), 5041: (40, 39563),
    #                   5042: (41, 797), 5043: (43, 8330), 5044: (45, 739), 5045: (42, 1577), 5046: (47, 1429),
    #                   5047: (40, 815), 5048: [(42, 10), (48, 1087)], 5049: (49, 792), 5050: (50, 854),
    #                   5051: (46, 1643), 5052: [(51, 271), (54, 9671), (55, 404)], 5053: (52, 564), 5054: (53, 601),
    #                   5056: (55, 2333), 5057: [(57, 4152), (61, 427), (62, 515)], 5058: (59, 625), 5059: (43, 1016),
    #                   5061: (60, 5577), 5062: (63, 622), 5063: (66, 556), 5064: (68, 4129), 5065: (68, 7563),
    #                   5066: (69, 1123), 5067: (70, 1363), 5068: (70, 830), 5070: (73, 936), 5071: (76, 771),
    #                   5072: (68, 558), 5073: (77, 6361), 5074: (79, 931), 5075: (80, 612), 5076: (81, 4991),
    #                   5077: (82, 37691), 5078: (83, 3128), 5079: (84, 583), 5080: (85, 590), 5081: (86, 12492),
    #                   5082: (87, 1895), 5083: (87, 2918), 5084: [(88, 333), (89, 696)], 5085: (90, 1344),
    #                   5086: (91, 4840), 5087: (92, 3753), 5088: (93, 928), 5089: (94, 2910), 5090: (96, 724),
    #                   5091: (98, 2752), 5092: (100, 757), 5093: [(103, 1321), (107, 1377)], 5094: (104, 2953),
    #                   5095: (106, 9383), 5096: (108, 866), 5097: (109, 722), 5098: (104, 127), 5099: (111, 565),
    #                   5100: (113, 8127), 5101: (112, 829), 5102: (114, 643), 5103: (115, 699), 5104: (116, 1178),
    #                   5105: (117, 795)}

    """
    After all potential overlaps are found we need to deal with several possibilities.
        1) Members of now_labs is not listed in direct_overlap.
            These represent newly formed tracks and need a label, add to live_ccl, so not confused with live_ccl or dead_ccl.
        2) Members of now_labs listed in direct_overlap.
            These represent extensions/continuations of existing tracks.
             Members of now_labs with only a single link are only linked to one member of last_labs and need to take on that label.
             Members of now_labs with only a multiple links to last_labs are mergers of existing tracks.
                How choose which last_labs label to keep and which to move to dead_ccl (i.e., terminated tracks.)?
        3) Members of last_labs not listed in direct_overlap.
            These represent terminated tracks and need to be moved from live_ccl to dead_ccl.
        4) Multiple members of now_labs point to the same member of last_labs.
            These represent a possible beginning of a splitting/forking of an existing track or
            multiple expansions/shifts in the spatial footprint of an existing track.
            For now, these now_labs are just remapped to the same last_labs CCL.
    """
    used_ccl = set()
    all_past_ccl = sorted(live_ccl + dead_ccl)
    used_ccl.update(all_past_ccl)
    # Ensure any new CCL tracks get unique value
    new_ccl = all_past_ccl[-1] + 1
    for nl in now_labs:
        if nl in direct_overlap:
            ##
            # Current CCL connects with Previous/existing CCL(s)
            if isinstance(direct_overlap[nl], tuple):
                ##
                # Only a single back-connection between nl and the previous timestep; extend existing track.
                if local_verboser:
                    mega_msg.append(f"\t\t{nl:4d} -> {direct_overlap[nl][0]:4d} Single-Link")
                ##
                # Update current CCL map
                lmap = np.where(lmap == nl, direct_overlap[nl][0], lmap)
                used_ccl.add(direct_overlap[nl][0])
            else:
                ##
                # Multiple back-connections between nl and the previous timestep; a track merger.
                if local_verboser:
                    mega_msg.append(f"\t\t{nl:4d} -> {direct_overlap[nl]} Multi-Link")
                ##
                # Find sizes of previous CCL (members of merge)
                sizes = [_[1] for _ in direct_overlap[nl]]
                max_idx = np.argmax(sizes)
                ##
                # Continue the largest of the previous CCL
                use_ll = direct_overlap[nl][max_idx][0]
                if local_verboser:
                    mega_msg.append(f"\t\t{nl:4d} -> {direct_overlap[nl][max_idx]} Selected")
                ##
                # Update current CCL map
                lmap = np.where(lmap == nl, use_ll, lmap)
                used_ccl.add(use_ll)
        else:
            # Current CCL doesn't connect with Previous/existing CCL(s); it is a new track
            if local_verboser:
                mega_msg.append(f"\t\t{nl:4d} -> {new_ccl:4d} New CCL")
            ##
            # Update current CCL map
            lmap = np.where(lmap == nl, new_ccl, lmap)
            used_ccl.add(new_ccl)
            new_ccl += 1
    now_labs = sorted(np.unique(lmap[lmap > 0]).tolist())
    now_labs_set = set(now_labs)
    if local_verbose:
        mega_msg.append(f"\t\tRemapped {now_labs = } ({len(now_labs)})")
    # print('\n'.join(mega_msg)); return [], [], []

    ##
    # Labels found in previous time-step but NOT now (i.e., Dead CCL)
    if local_verbose:
         mega_msg.append("\t\tLooking for new dead labels")
    dead_ccl_set = set(dead_ccl)
    killed_labs = sorted(list(last_labs_set.difference(now_labs_set)))
    if local_verbose:
         mega_msg.append(f"\t\t\t{killed_labs = } ({len(killed_labs)})")
    if killed_labs:
        killed_labs_set = set(killed_labs)
        killed_labs = sorted(list(killed_labs_set.difference(dead_ccl_set)))
        if local_verbose:
             mega_msg.append(f"\t\t\t*{killed_labs = } ({len(killed_labs)})")
    if killed_labs:
        # Ensure killed_labs not already in dead_ccl
        killed_labs_set = set(killed_labs)
        killed_labs = sorted(list(killed_labs_set.difference(dead_ccl_set)))
        if local_verbose:
             mega_msg.append(f"\t\t\t**{killed_labs = } ({len(killed_labs)})")
             mega_msg.append(f"\t\t\t{dead_ccl = } ({len(dead_ccl)})")
             mega_msg.append(f"\t\t\t{live_ccl = } ({len(live_ccl)})")
        # Is killed_labs conflicting with dead_ccl?
        kmap = {}
        all_past_ccl = sorted(live_ccl + dead_ccl)
        for kl in killed_labs:
            if kl in dead_ccl:
                # kl already used. Assign it a new label not one already in dead_ccl or live_ccl
                nu_lab = all_past_ccl[-1] + 1
                kmap[kl] = nu_lab
                dead_ccl.append(nu_lab)
            else:
                dead_ccl.append(kl)
        dead_ccl = sorted(list(set(dead_ccl)))
        killed_labs = [(kmap[_] if _ in kmap else _) for _ in killed_labs]
        live_ccl = [_ for _ in live_ccl if _ not in killed_labs]
        if local_verbose:
             mega_msg.append(f"\t\t\t***{killed_labs = } ({len(killed_labs)})")
             mega_msg.append(f"\t\t\t*{dead_ccl = } ({len(dead_ccl)})")
             mega_msg.append(f"\t\t\t*{live_ccl = } ({len(live_ccl)})")

    ##
    # Labels found now but NOT in previous time-step (i.e., New CCL)
    new_ccl = sorted(list(now_labs_set.difference(last_labs_set)))
    # Is new_ccl conflicting with live_ccl?
    nmap = {}
    all_past_ccl = sorted(live_ccl + dead_ccl)
    if local_verbose:
         mega_msg.append("\t\tLooking for new labels")
         mega_msg.append(f"\t\t\t{all_past_ccl = } ({len(all_past_ccl)})")
    if new_ccl:
        if local_verbose:
             mega_msg.append(f"\t\t\t{new_ccl = } ({len(new_ccl)})")
        for nl in new_ccl[::-1]:
            # Check if nl used in past
            if nl in all_past_ccl:
                # nl already used. Assign it a new label not one already in all_past_ccl
                nu_lab = all_past_ccl[-1] + 1
                nmap[nl] = nu_lab
                live_ccl.append(nu_lab)
            else:
                # No conflict
                live_ccl.append(nl)
        for nl in nmap.keys():
            new_ccl = [(nmap[nl] if _ == nl else _) for _ in new_ccl]
            new_ccl = sorted(new_ccl)
            lmap = np.where(lmap == nl, nmap[nl], lmap)
        live_ccl = sorted(list(set(live_ccl)))
        if local_verbose:
             mega_msg.append(f"\t\t\t*{new_ccl = } ({len(new_ccl)})")
             mega_msg.append(f"\t\t\t*{live_ccl = } ({len(live_ccl)})")

    # print('\n'.join(mega_msg)); return [], [], []

    # # ## *
    # # # Rename any conflicted_labs
    # # re_name = {}
    # # if conflicted_labs:
    # #     for clab_past in conflicted_labs:
    # #         local_verboseif local_verbose:
    # #             print(f"\tPast {clab_past}")
    # #         for clab_now in conflicted_labs:
    # #             if local_verbose:
    # #                 print(f"\t\tNow  {clab_now}")
    # #             ##
    # #             # Does the current clab intersect with the clab_past?
    # #             cmap_hit = np.asarray((lmap == clab_now) & (lmap_prev == clab_past)).nonzero()
    # #             if local_verbose:
    # #                 print(f"\t\t\t{cmap_hit  = }")
    # #             if cmap_hit[0].tolist():
    # #                 re_name[clab_now] = clab_past
    # #                 if local_verbose:
    # #                     print(f"\t\t\tCurrent {clab_now} -> Past {clab_past}")
    # #     lmap_old = np.copy(lmap)
    # #     for clab_ in conflicted_labs[::-1]:
    # #         if clab_ in re_name:
    # #             lmap = np.where(lmap_old == clab_, re_name[clab_], lmap)
    # #     now_labs = sorted(np.unique(lmap[lmap > 0]).tolist())
    # #     if local_verbose:
    # #         print(f"\t\tNew {now_labs = }")

    # if local_verbose:
    #     print('\n'.join(mega_msg))

    return lmap, live_ccl, dead_ccl, mega_msg

###############################################################################
# PUBLIC get_ccl()
# ----------------
def get_ccl(tidx: int, now_mask: npt.ArrayLike, min_voxels: int, use_connectivity: int, sname: str) -> str:
    print(f"\tDoing {tidx:4d} ... ", end='')
    ##
    # Find current 2D CCL
    ccl_map = cc3d.connected_components(now_mask, delta=0, connectivity=use_connectivity, return_N=False)
    ##
    # Mask if voxel count is too low
    now_ccl = sorted(np.unique(ccl_map[ccl_map > 0]).tolist())
    for clab in now_ccl:
        ##
        # Mask where clab located
        cl_mask = np.where(ccl_map == clab, 1, 0)
        cl_voxels = int(np.count_nonzero(cl_mask))
        if cl_voxels < min_voxels:
            # Drop CCL label
            ccl_map = np.where(ccl_map == clab, 0, ccl_map)
    ##
    # Edge Check of current CCL
    ccl_map, map_replace = edge_check(ccl_map)

    # Save to File
    np.save(sname, ccl_map)
    print(" Done")

    return sname

###############################################################################
# PUBLIC pf_search()
# ------------------
def pf_search(flist: list[str], dt_str: list[str], dyamond_mask_file: str, use_connectivity: int, dyamond_ccl_initial_file: str, dyamond_ccl_file: str, hidden_path: str, min_voxels: int, just_48: bool) -> None:
    """ """
    mega_msg = []
    local_verbose = [False, True][0]
    make_mask = [False, True][1]
    find_ccl = [False, True][0]
    ncores = 4
    track_ccl = [False, True][0]

    ##
    # Define data grid

    # grid mid-points [-89.95, 89.95] so to -90 to +90 with edges
    #   lats = [-89.95, -89.85, ... -0.05, 0.05, ... 89.85, 89.95]
    nlats = 1800
    dlat = 0.1
    lats = np.arange(-89.95, 90.0, dlat)

    # grid mid-points [-179.95, 179.95] so to -180 to +180 with edges
    #   lons = [-179.95, -179.85, ... -0.05, 0.05, ... 179.85, 179.95]
    nlons = 3600
    dlon = 0.1
    lons = np.arange(-179.95, 180.0, dlon)

    ##
    # Find row separating NH and SH
    difference_array = np.absolute(lats - 0)
    # find the index of minimum element from the array
    nh_0 = difference_array.argmin()
    sh_0 = nh_0 - 1
    # print(f"{sh_0} {lats[sh_0]}")
    # print(f"{nh_0} {lats[nh_0]}")
    maxid = nlats * nlons
    row_start = tuple([_ for _ in range(maxid) if _ % nlons == 0])
    row_end = tuple([_ + (nlons - 1) for _ in row_start])
    # (3239999, 3240000) (row_end[sh_0], row_start[nh_0])
    hemi_sep = (row_end[sh_0], row_start[nh_0])
    # print(hemi_sep)

    ntimes = len(flist)

    # Note changes to pr_floor require rerunning make_mask
    pr_floor = 0.1 / 3600.0; floor_tag = "01mmhr"   # 0.1 mm/hr as mm/s
    # pr_floor = 0.25 / 3600.0; floor_tag = "025mmhr"  # 0.25 mm/hr as mm/s
    # pr_floor = 0.5 / 3600.0; floor_tag = "05mmhr"  # 0.5 mm/hr as mm/s
    # pr_floor = 1.0 / 3600.0; floor_tag = "1mmhr"  # 1 mm/hr as mm/s
    # pr_floor = 10.0 / 3600.0 # 10 mm/hr as mm/s
    # pr_floor = 20.0 / 3600.0 # 100 mm/hr as mm/s

    dyamond_mask_file = dyamond_mask_file.replace(".npy", f"_{floor_tag}.npy")
    dyamond_ccl_initial_file = dyamond_ccl_initial_file.replace(".pkl", f"_{floor_tag}.pkl")
    dyamond_ccl_file = dyamond_ccl_file.replace(".pkl", f"_{floor_tag}.pkl")
    if just_48:
        dyamond_mask_file = dyamond_mask_file.replace(".npy", "_48.npy")
        dyamond_ccl_initial_file = dyamond_ccl_initial_file.replace(".pkl", "_48.pkl")
        dyamond_ccl_file = dyamond_ccl_file.replace(".pkl", "_48.pkl")

    dl_lon_idx = 1800 # -0.05 0.05 0.15

    times = np.zeros(ntimes, dtype='datetime64[m]')
    for tidx, atime_str in enumerate(dt_str):
        # print(f"{tidx:4d} {atime_str}")
        tmp_ = atime_str.split(" ")
        tmp__ = tmp_[-2].split(":")
        times[tidx] = np.datetime64(f'{tmp_[0]}-{tmp_[1]}-{tmp_[2]}T{tmp__[0]}:{tmp__[1]}')

    # Best dtype to minimize array memory footprint
    #
    #   Note: (see numpy.result_type)
    #       * When both scalars and arrays are used, the array’s type takes precedence and the actual value of the scalar is taken into account.
    #       * When two arrays are used in a numpy calculaton 'min_scalar_type' is called on each array and the resulting data types are all
    #           combined with 'promote_types' to produce the return value. Many times this results in native 64-bit results.
    #
    # Masks usually hold positive integer 0/1 or bool
    #    8-bit boolean          (bool, 'True'/'False', 1/0) has 1/8 the memory footprint of int64/float64
    #    8-bit unsigned integer (uint8, 0 to 255)           has 1/8 the memory footprint of int64/float64
    #   16-bit unsigned integer (uint16, 0 to 65535)        has 1/4 the memory footprint of int64/float64
    #   32-bit unsigned integer (uint32, 0 to 4294967295)   has 1/2 the memory footprint of int64/float64
    #   i4 = int32  f4 = float32    32-bit integer and floating-point number
    #   i8 = int64  f8 = float64    64-bit integer and floating-point number
    mask_dtype = bool
    mask_dtype_alt = np.uint8
    float_dtype_alt = np.float32
    int_dtype_alt = np.int32
    int_dtype_alt = np.int16

    ##
    # Read raw IMERG files, make 0/1 mask and save.
    if make_mask:
        # (ntimes, nlats, nlons)
        # pr = np.zeros((ntimes, nlats, nlons), dtype=np.float32)
        masked_pr_flag = np.zeros((ntimes, nlats, nlons), dtype=mask_dtype)
        print("\nMaking Mask... ")
        loop_src = flist if local_verbose else tqdm(flist, total=ntimes, desc=f"Reading NetCDF")
        for midx, mfile in enumerate(loop_src):
            if local_verbose:
                print(f"{midx: 03d}: {mfile}")

            ##
            # Open a file
            # ds = netCDF4.Dataset(mfile)
            ds = h5py.File(mfile + '.carved', 'r')["PRECTOT"]

            ##
            # Read PR Field
            #   (1, 1800, 3600) numpy.ma.core.MaskedArray numpy.float32
            _pr = ds[()]
            #   (1800, 3600)

            point_selection_mask = (_pr >= pr_floor)
            ds[point_selection_mask]

            _pr = _pr.squeeze()

            ##
            # Remove mask
            _pr = ma.filled(_pr, 0)

            #ds.close()

            ##
            # Mask with PR < 0.1 mm/hr
            _pr = np.where(_pr >= pr_floor, 1, 0)

            # pr_mask = np.nonzero(_pr >= pr_floor)
            pr_mask = np.nonzero(_pr)
            # if midx == 0:
            #     # Save as netcdf
            #     dyamond_mask_file_nc = dyamond_mask_file.replace(".npy", ".nc")
            #     ncfile = netCDF4.Dataset(dyamond_mask_file_nc, mode='w', format='NETCDF4_CLASSIC')
            #     lat_dim = ncfile.createDimension('lat', nlats) # latitude axis
            #     lon_dim = ncfile.createDimension('lon', nlons) # longitude axis
            #     lat = ncfile.createVariable('lat', np.float32, ('lat',))
            #     lat.units = 'degrees_north'
            #     lat.long_name = 'latitude'
            #     lon = ncfile.createVariable('lon', np.float32, ('lon',))
            #     lon.units = 'degrees_east'
            #     lon.long_name = 'longitude'
            #     cclmap = ncfile.createVariable('cclmap', np.float32, ('lat', 'lon'))
            #     lat[:] = lats
            #     lon[:] = lons
            #     cclmap[:, :] = _pr
            #     ncfile.close()

            del _pr
            masked_pr_flag[midx, pr_mask[0], pr_mask[1]] = True
            del pr_mask

            # break

        ##
        # Roll so dateline is not on the map edges
        # masked_pr_flag = np.roll(masked_pr_flag, dl_lon_idx, axis=2)

        # ##
        # # Trim off Spatial extras
        # #   (4320, 900, 3600) -> (4320, :t_edge + 1, l_edge:r_edge + 1) or (4320, 701, 1400)
        # r_edge = 2599
        # l_edge = 1200
        # t_edge = 700
        # masked_pr_flag = masked_pr_flag[:, :t_edge + 1, l_edge:r_edge + 1]

        ##
        # Save
        print(f"Saving Mask to {dyamond_mask_file}")
        np.save(dyamond_mask_file, masked_pr_flag)

        print("\n\tRerun with make_mask == False\n")
        os._exit(0)
    else:
        if find_ccl:
            print("\nReading Mask... ")
            masked_pr_flag = np.load(dyamond_mask_file)

    if find_ccl:
        # ##
        # # Find current 3D CCL
        # print("\nFinding CCL Field... ")
        # # ccl_map = cc3d.connected_components(masked_pr_flag, delta=0, connectivity=use_connectivity, return_N=False)
        # # ccl_map = cc3d.dust(ccl_map, threshold=20000, connectivity=use_connectivity, in_place=True)
        # ##
        # # Save
        # np.save(dyamond_ccl_file, ccl_map)

        ##
        # Find current 2D CCL and connect over time and wrap around dateline
        #   Store CCL (features) as timeseries map for time tracking.
        #   Each day (48 files) takes ~min on 8-core i7 CPU
        print(f"\nFinding CCL Field... for {ntimes}")
        ccl_files = []
        with Pool(ncores) as pool:
            results = pool.starmap(get_ccl, [(tidx, masked_pr_flag[tidx, :, :], min_voxels, use_connectivity, f"{hidden_path}get_ccl_{min_voxels:03d}_{floor_tag}_{tidx:05d}.npy") for tidx in range(ntimes)])
            for res in results:
                ccl_files.append(res)
        del res, results
        print("Saved {len(ccl_files)}")

        ##
        # Relabel so contiguous range within time step
        ccl_super_map = np.zeros((ntimes, nlats, nlons), dtype=np.int16)
        for tidx in range(ntimes):
            cclf = f"{hidden_path}get_ccl_{min_voxels:03d}_{floor_tag}_{tidx:05d}.npy"
            print(f"Reading {cclf}")
            ccl_map = np.load(cclf)
            now_ccl = sorted(np.unique(ccl_map[ccl_map > 0]).tolist())
            # print(f"{now_ccl = }")
            # Offset so can rename without conflicts
            new_ccl = now_ccl[-1]
            for old_ccl in now_ccl:
                new_ccl += 1
                # print(f"{old_ccl = } {new_ccl = }")
                ccl_map[ccl_map == old_ccl] = new_ccl
            # Remove offset so first CCL == 1
            ccl_map = np.where(ccl_map > 0, ccl_map - now_ccl[-1], 0)
            ccl_super_map[tidx, :, :] = ccl_map
        ##
        # Save
        print(f"Saved {dyamond_ccl_initial_file}")
        with open(dyamond_ccl_initial_file, 'wb') as f:
            pickle.dump(ccl_super_map, f)
    else:
        if track_ccl:
            print("\nReading CCL Field... ")
            with open(dyamond_ccl_initial_file, 'rb') as f:
                ccl_super_map = pickle.load(f)

    # ##
    # # Debug
    # tidx = 0
    # ccl_map = ccl_super_map[tidx, :, :]
    # now_ccl = sorted(np.unique(ccl_map[ccl_map > 0]).tolist())
    # n_ccl = len(now_ccl)
    # print(f"\tnow_ccl  ({n_ccl:4d}): {now_ccl}\n\n")
    # basic_plot(ccl_map, lons, lats, str(np.datetime_as_string(times[tidx], unit='m')), f"{hidden_path}ccl_map_{min_voxels:03d}_{floor_tag}_{tidx:04d}.png")
    # os._exit(0)

    if track_ccl:
        # # Debug
        # for tidx in range(ntimes):
        #     ccl_mapb = ccl_super_map[tidx, :, :]
        #     now_ccl = sorted(np.unique(ccl_mapb[ccl_mapb > 0]).tolist())
        #     n_ccl = len(now_ccl)
        #     print(f"\tnow_ccl  ({n_ccl:4d}): {now_ccl}\n\n")
        #     for cc in now_ccl:
        #         ccl_mapc = np.where(ccl_mapb == int(cc), ccl_mapb, 0)
        #         print(f"{cc:5d} = {np.count_nonzero(ccl_mapc)}")
        #         basic_plot(ccl_mapc, lons, lats, str(np.datetime_as_string(times[tidx], unit='m')), f"{hidden_path}ccl_map_{tidx:04d}_ccl{int(cc):05d}.png")
        #     if tidx == 1:
        #         os._exit(1)
        # os._exit(1)

        # for tidx in range(ntimes):
        #     ccl_mapb = ccl_super_map[tidx, :, :]
        #     now_ccl = sorted(np.unique(ccl_mapb[ccl_mapb > 0]).tolist())
        #     n_ccl = len(now_ccl)
        #     print(f"\tnow_ccl  ({n_ccl:4d}): {now_ccl}\n\n")
        #     basic_plot(ccl_mapb, lons, lats, str(np.datetime_as_string(times[tidx], unit='m')), f"{hidden_path}ccl_map_{tidx:04d}.png")
        #     if tidx == 2:
        #         os._exit(1)
        # os._exit(1)

        ##
        # Find current 2D CCL and connect over time and wrap around dateline
        # Stored CCL (features) up to the previous time step
        live_ccl = []
        # Stored CCL (features) that have been retired (i.e., not present in the previous time step)
        dead_ccl = []
        # ccl_final_map = np.zeros((ntimes, nlats, nlons), dtype=np.int16)
        prev_ccl_map = []
        # All Stored CCL (features) = live_ccl + dead_ccl
        loop_src = range(ntimes) if local_verbose else tqdm(range(ntimes), desc=f"Finding CCL")
        for tidx in loop_src:
            if local_verbose:
                mega_msg.append(f"\nTime Index {tidx:3d}")
                if live_ccl:
                    mega_msg.append(f"\tlive_ccl ({len(live_ccl)}): [{live_ccl[0]} ... {live_ccl[-1]}]")
                else:
                    mega_msg.append(f"\tlive_ccl ({len(live_ccl)})")
                if dead_ccl:
                    mega_msg.append(f"\tdead_ccl ({len(dead_ccl)}): [{dead_ccl[0]} ... {dead_ccl[-1]}]")
                else:
                    mega_msg.append(f"\tdead_ccl ({len(dead_ccl)})")
            ccl_map = ccl_super_map[tidx, :, :]
            now_ccl = sorted(np.unique(ccl_map[ccl_map > 0]).tolist())

            # # Debug limit to 5 CCL
            # now_ccl = now_ccl[:5]
            # new_ccl_map = np.zeros((nlats, nlons), dtype=np.int16)
            # for nl in now_ccl:
            #     new_ccl_map = np.where(ccl_map == nl, ccl_map, new_ccl_map)
            # ccl_map = new_ccl_map
            # now_ccl = sorted(np.unique(ccl_map[ccl_map > 0]).tolist())
            # n_ccl = len(now_ccl)
            # print(f"\tnow_ccl pre-tracking  ({n_ccl:4d}): [{now_ccl[0]} ... {now_ccl[-1]}]")

            if local_verbose:
                n_ccl = len(now_ccl)
                if now_ccl:
                    mega_msg.append(f"\tnow_ccl pre-tracking  ({n_ccl:4d}): [{now_ccl[0]} ... {now_ccl[-1]}]")
                else:
                    mega_msg.append(f"\tnow_ccl pre-tracking  ({n_ccl:4d}):")
            if tidx == 0:
                ##
                # Initial Pass, all current CCL see as live (new features)
                live_ccl = now_ccl[:]
                prev_ccl_map = ccl_map
                ##
                # Store the CCL map (features)
                cclf = f"{hidden_path}ccl_map_{min_voxels:03d}_{floor_tag}_{tidx:05d}.npy"
                np.save(cclf, ccl_map)
                continue

            ##
            # Connect/track new CCL to live features or create new features.
            #   Updates ccl_map, live_ccl and dead_ccl
            ccl_map, live_ccl, dead_ccl, msg = track_labels(prev_ccl_map, ccl_map, live_ccl, dead_ccl, hemi_sep)
            if msg:
                mega_msg.extend(msg)
            now_ccl = sorted(np.unique(ccl_map[ccl_map > 0]).tolist())
            n_ccl = len(now_ccl)
            prev_ccl_map = ccl_map

            # # Debug
            # basic_plot(ccl_map, lons, lats, str(np.datetime_as_string(times[tidx], unit='m')), f"{hidden_path}ccl_map_{tidx:04d}.png")
            # # Save as netcdf
            # dyamond_mask_file_nc = f"{hidden_path}ccl_map_{tidx:04d}.nc"
            # ncfile = netCDF4.Dataset(dyamond_mask_file_nc, mode='w', format='NETCDF4_CLASSIC')
            # lat_dim = ncfile.createDimension('lat', nlats) # latitude axis
            # lon_dim = ncfile.createDimension('lon', nlons) # longitude axis
            # lat = ncfile.createVariable('lat', np.float32, ('lat',))
            # lat.units = 'degrees_north'
            # lat.long_name = 'latitude'
            # lon = ncfile.createVariable('lon', np.float32, ('lon',))
            # lon.units = 'degrees_east'
            # lon.long_name = 'longitude'
            # cclmap = ncfile.createVariable('cclmap', np.float32, ('lat', 'lon'))
            # lat[:] = lats
            # lon[:] = lons
            # cclmap[:, :] = ccl_map
            # ncfile.close()
            # break

            ##
            # Store the CCL map (features)
            cclf = f"{hidden_path}ccl_map_{min_voxels:03d}_{floor_tag}_{tidx:05d}.npy"
            np.save(cclf, ccl_map)
            if local_verbose:
                mega_msg.append(f"\tTracked now_ccl ({n_ccl}): -> {cclf}")
                if now_ccl:
                    mega_msg.append(f"\tnow_ccl  ({n_ccl:4d}): [{now_ccl[0]} ... {now_ccl[-1]}]")
                else:
                    mega_msg.append(f"\tnow_ccl  ({n_ccl:4d}):")
                if live_ccl:
                    mega_msg.append(f"\tlive_ccl ({len(live_ccl):4d}): [{live_ccl[0]} ... {live_ccl[-1]}]")
                else:
                    mega_msg.append(f"\tlive_ccl ({len(live_ccl):4d}):")
                if dead_ccl:
                    mega_msg.append(f"\tdead_ccl ({len(dead_ccl):4d}): [{dead_ccl[0]} ... {dead_ccl[-1]}]")
                else:
                    mega_msg.append(f"\tdead_ccl ({len(dead_ccl):4d}):")
    else:
        # # ccl_final_map = np.zeros((ntimes, nlats, nlons), dtype=np.int16)
        # loop_src = range(ntimes) if local_verbose else tqdm(range(ntimes), desc=f"Finding CCL")
        # for tidx in loop_src:
        #     cclf = f"{hidden_path}ccl_map_{min_voxels:03d}_{floor_tag}_{tidx:05d}.npy"
        #     ccl_map = np.load(cclf)
        #     # ccl_final_map[tidx, :, :] = ccl_map

        #     now_ccl = sorted(np.unique(ccl_map[ccl_map > 0]).tolist())
        #     n_ccl = len(now_ccl)
        #     mega_msg.append(f"{tidx:5d} {n_ccl:5d} {now_ccl[0]:5d} {now_ccl[-1]:5d} {str(np.datetime_as_string(times[tidx], unit='m'))}")

        #     # Debug
        #     # basic_plot(ccl_map, lons, lats, str(np.datetime_as_string(times[tidx], unit='m')), f"{hidden_path}tracked_ccl_map_{tidx:04d}.png")

        #     # if tidx > 10:
        #     #     break
        # print('\n'.join(mega_msg)); return [], [], []


        # ffmpeg -framerate 48 -pattern_type glob -i '*.png' -vcodec libx264 -pix_fmt yuv420p -s 1920x1080 -crf 0 ccl_1080p.mp4

        with Pool(ncores) as pool:
            results = pool.starmap(basic_plot_pool, [(f"{hidden_path}ccl_map_{min_voxels:03d}_{floor_tag}_{tidx:05d}.npy", lons, lats,  str(np.datetime_as_string(times[tidx], unit='m')), f"{hidden_path}tracked_ccl_map_{tidx:04d}.png") for tidx in range(ntimes)])
            for res in results:
                print(f"Done {res}")
        del res, results



    #     ##
    #     # Save
    #     mega_msg.append(f"Saved {dyamond_ccl_file}")
    #     with open(dyamond_ccl_file, 'wb') as f:
    #         pickle.dump(ccl_final_map, f)
    # else:
    #     with open(dyamond_ccl_file, 'rb') as f:
    #         ccl_final_map = pickle.load(f)

    print('\n'.join(mega_msg)); return [], [], []

    # for tidx in range(ntimes):
    #     imerg_map = ccl_map[tidx, :, :]
    #     now_ccl = sorted(np.unique(imerg_map[imerg_map > 0]).tolist())
    #     print(f"{tidx:4d} Now CCL {len(now_ccl)}")

    #     pname = f"{hidden_path}ccl_{tidx:04d}.png"
    #     a_time_str = str(np.datetime_as_string(times[tidx], unit='m'))
    #     basic_plot(imerg_map, lons, lats, f'{a_time_str.replace("T", "-")} UTC', pname)
    #     break

    return

# ---Start of main code block.
if __name__ == '__main__':

    key_var = "PRECTOT" # [kg m-2 s-1] or [mm/s]
    just_48 = [False, True][0]

    ##
    # Get DYAMONDv2 files
    base_data_path = ["/shared/hdf5_selections/POMD/discover/"][0]
    dir_list = ("202001", "202002")
    hidden_path = ["/shared/hdf5_selections/POMD/output/"][0]

    # Connectivity for CC3D: only 4, 8 (2D) and 26, 18 and 6 (3D)
    use_connectivity = 26
    use_connectivity = 8
    ##
    # Min time-space ccl retention size (pixels)
    #   IMERG has a 0.1 deg by 0.1 deg grid, so a square area of IMERG grids is uses for a rough length scale
    #     min_voxels = 625 or 25x25 grids or 2.5 deg by 2.5 deg or a length scale of 277.5 km, which is well below mesoscale (500-1000 km)
    #                  400 or 20x20 grids or 2.0 deg by 2.0 deg or                   222.0 km
    #                  225 or 15x15 grids or 1.5 deg by 1.5 deg or                   166.5 km
    #                  100 or 10x10 grids or 1.0 deg by 1.0 deg or                   111.0 km
    #
    min_voxels = [625, 400, 225, 100, 0][0]
    dyamond_ccl_initial_file = f"{hidden_path}dyamond_ccl_{use_connectivity:02d}_{min_voxels:03d}.pkl"
    dyamond_ccl_file = f"{hidden_path}dyamond_ccl_{use_connectivity:02d}_{min_voxels:03d}.pkl"
    dyamond_mask_file = f"{hidden_path}dyamond_raw_pr_{min_voxels:03d}.npy"

    ##
    # Find all files
    file_list = sorted([f"{base_data_path}{base_path}/{_}" for base_path in dir_list for _ in os.listdir(f"{base_data_path}{base_path}/") if _.endswith('.nc4.carved')])

    if just_48:
        # TMP Limit to a single day
        file_list = file_list[:48]
        # file_list = file_list[:1]

    nfiles = len(file_list)
    # print(f"Found {nfiles} Files: [{file_list[0]} ... {file_list[-1]}]")

    ##
    # Extract Datetimes
    dtime_str = []
    dtime_dt = []
    for afile in file_list:
        tmp_ = afile.split("/")[-1]
        tmpa_ = tmp_.split(".")[-2]
        tmpb_ = tmpa_.split("_")
        yyyy_str = tmpb_[0][:4]
        mm_str = tmpb_[0][4:6]
        dd_str = tmpb_[0][6:8]
        hh_str = tmpb_[1][:2]
        min_str = tmpb_[1][2:4]
        # print(f"YYYY {yyyy_str} MM {mm_str} DD {dd_str} HH {hh_str} Min {min_str}")

        dtime_str.append(f"{yyyy_str} {mm_str} {dd_str} {hh_str}:{min_str} UTC")
        dtime_dt.append(np.datetime64(f'{yyyy_str}-{mm_str}-{dd_str}T{hh_str}:{min_str}'))

    pf_search(file_list, dtime_str, dyamond_mask_file, use_connectivity, dyamond_ccl_initial_file,
              dyamond_ccl_file, hidden_path, min_voxels, just_48)

# >>>> ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: <<<<
# >>>> END OF FILE | END OF FILE | END OF FILE | END OF FILE | END OF FILE | END OF FILE <<<<
# >>>> ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: <<<<
