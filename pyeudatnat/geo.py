#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.. _geo

.. Links

.. _happygisco: https://github.com/geopy/geopy
.. |happygisco| replace:: `happygisco <happygisco_>`_
.. _geopy: https://github.com/geopy/geopy
.. |geopy| replace:: `geopy <geopy_>`_
.. _pyproj: https://github.com/pyproj4/pyproj)
.. |pyproj| replace:: `pyproj <pyproj_>`_

Module implementing miscenalleous useful methods, including translation and text
processing.

**Dependencies**

*require*:      :mod:`os`, :mod:`six`, :mod:`collections`, :mod:`numpy`, :mod:`pandas`

*optional*:     :mod:`geopy`, :mod:`happygisco`, :mod:`pyproj`

*call*:         :mod:`pyeudatnat`

**Contents**
"""

# *credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_
# *since*:        Thu Apr  9 09:56:45 2020

#%% Settings

import io, sys
from os import path as osp
import logging

from collections import OrderedDict, Mapping, Sequence#analysis:ignore
import functools, itertools
from six import string_types
from uuid import uuid4

import zipfile

import numpy as np#analysis:ignore
import pandas as pd#analysis:ignore

try:
    from osgeo import gdal, gdal_array, gdalconst
    from osgeo import osr, ogr
except ImportError:
    try:
        import gdal, gdal_array, gdalconst
        import ogr, osr
    except ImportError:
        # logging.warning('\n! Missing gdal package (https://pcjericks.github.io/py-gdalogr-cookbook/index.html) !')
        class gdal():
            GetDriverCount = lambda *args: 0
            GetDriver = lambda *args: None

try:
    import rasterio
except ImportError:
    pass

try:
    import shapely
    from shapely import wkb, geometry
except ImportError:
    pass

try:
    assert False
    import happygisco#analysis:ignore
except (AssertionError,ImportError):
    _is_happy_installed = False
    # logging.warning('\n! Missing happygisco package (https://github.com/eurostat/happyGISCO) - GISCO web services not available !')
else:
    # logging.warning('\n! happygisco help: hhttp://happygisco.readthedocs.io/ !')
    _is_happy_installed = True
    from happygisco import services
    # CODERS = {'GISCO':None, 'osm':None})

try:
    import geopy#analysis:ignore
except ImportError:
    _is_geopy_installed = False
    # logging.warning('\n! Missing geopy package (https://github.com/geopy/geopy) !')
else:
    # logging.warning('\n! geopy help: http://geopy.readthedocs.io/en/latest/ !')
    _is_geopy_installed = True
    # from geopy import geocoders
    #CODERS.update({'GoogleV3':'api_key', 'Bing':'api_key', 'GeoNames':'username',
    #               'Yandex':'api_key', 'MapQuest':'key', 'Nominatim':None,
    #               'OpenMapQuest':'api_key'})

try:
    assert _is_happy_installed is True or _is_geopy_installed is True
except AssertionError:
    # raise IOError('no geocoding module available')
    logging.warning('\n! No geocoding module available !')

try:
    import pyproj#analysis:ignore
except ImportError:
    # logging.warning('\n! missing pyproj package (https://github.com/pyproj4/pyproj) !')
    _is_pyproj_installed = False
else:
    # logging.warning('\n! pyproj help: https://pyproj4.github.io/pyproj/latest/ !')
    _is_pyproj_installed = True
    from pyproj import CRS as crs, Transformer

from pyeudatnat import PACKNAME, COUNTRIES
from pyeudatnat.misc import FileSys

__CODERS        = { }
CODERS          = __CODERS                                                           \
                    .update({'GISCO':           None,  # osm and GISCO) are actually... Nominatim on GISCO servers
                             'osm':             None} if _is_happy_installed else {})           \
                    or __CODERS                                                             \
                    .update({'GoogleV3':        'api_key', # since July 2018 Google requires each request to have an API key
                             'Bing':            'api_key',
                             'GeoNames':        'username',
                             'Yandex':          'api_key',
                             'MapQuest':        'key',
                             'Nominatim':       None, # using Nominatim with the default geopy is strongly discouraged
                             'OpenMapQuest':    'api_key'} if _is_geopy_installed else {})      \
                    or __CODERS # at the end, CODERS will be equal to __CODERS after its updates

# default geocoder... but this can be reset when declaring a subclass
DEF_CODER       = {'Nominatim': None} # {'Bing' : None}

DEF_AGENT       = PACKNAME

DRIVERS         = {gdal.GetDriver(i).ShortName: gdal.GetDriver(i).LongName
                   for i in range(gdal.GetDriverCount())}

DEF_DRIVER      = "GeoJSON"

DEF_PROJ        = 'WGS84'
DEF_PROJ4LL     = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'
DEF_PROJ4SM     = '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +no_defs'


# LATLON        = ['lat', 'lon'] # 'coord' # 'latlon'
# ORDER         = 'lL' # first lat, second Lon

DEF_PLACE       = ['street', 'number', 'postcode', 'city', 'country']
"""Fields used to defined a toponomy (location/place).
"""

#%% Core functions/classes

#==============================================================================
# Method isoCountry
#==============================================================================

def isoCountry(arg):
    """Given a country name or an ISO 3166 code, return the pair {name,code}.

        >>> country = isoCountry(country_or_cc)

    Examples
    --------

        >>> geo.isoCountry('CZ')
            {'code': 'CZ', 'country': 'Czechia'}
        >>> geo.isoCountry('Greece')
            {'code': 'EL', 'country': 'Greece'}
    """
    country, cc = None, None
    if not (arg is None or isinstance(arg, (string_types,Mapping))):
        raise TypeError("Wrong format for country/code '%s' - must be a string or a dictionary" % arg)
    elif isinstance(arg, string_types):
        if arg in COUNTRIES.keys():
            cc, country = arg, None
        elif arg in COUNTRIES.values():
            cc, country = None, arg
        else:
            raise IOError("Country/code '%s' not recognised" % arg)
    elif isinstance(arg, Mapping):
        if 'code' in arg or 'name' in arg:
            cc, country = arg.get('code', None), arg.get('name', None)
        else:
            try:
                cc, country = list(next(iter(arg.items()))) # list(arg.items())[0]
            except:
                pass
    if cc in ('', None) and country in ('', {}, None):
        raise IOError("Missing parameters to define country/code")
    elif cc in ('', None): # and NOT country in ('', {}, None)
        try:
            cc = dict(map(reversed, COUNTRIES.items())).get(country)
        except:
            #cc = country.split()
            #if len(cc) >1:              cc = ''.join([c[0].upper() for c in country.split()])
            #else:                       cc = country # cc[0]
            cc = None
    elif country in ('', {}, None): # and NOT cc in ('', None)
        try:
            country = COUNTRIES.get(cc)
        except:     country = None
    return {'code': cc, 'name': country}


#==============================================================================
# Class Service
#==============================================================================

class Service(object):
    """Instantiation class for geoprocessing module.

        >>> geoserv = Service()
    """

    #/************************************************************************/
    @classmethod
    def get_client(cls, arg):
        """Define geocoder client.

            >>> coder = Service.get_client(arg)
        """
        if arg is None:
            #arg = cls.DEF_CODER.copy()
            raise IOError("No geocoder parsed")
        elif not isinstance(arg, (string_types,Mapping)):
            raise TypeError("Wrong format for geocoder '%s' - must be a string or a dictionary" % arg)
        elif isinstance(arg, string_types):
            coder, key = arg, None
        elif isinstance(arg, Mapping):
            if 'coder' in arg and arg['coder'] in CODERS.keys():
                # arg already of the form: {'coder': coder, CODERS[coder]: key}
                return arg
            else:
                coder, key = list(arg.items())[0]
        try:
            assert coder in CODERS
        except:
            raise IOError("Geocoder '%s' not available/recognised" % coder)
        try:
            assert _is_happy_installed is True or coder.lower() not in ('osm','gisco')
        except:
            try:
                assert _is_geopy_installed is True
            except:
                raise IOError("No geocoder available")
            else:
                coder, key = DEF_CODER.items()
        coder = {'coder': coder, CODERS[coder]: key}
        coder.pop(None, None)
        return coder

    #/************************************************************************/
    @classmethod
    def get_geoclient(cls, *args, **kwargs):
        try:
            assert _is_geopy_installed is True
        except:
            raise ImportError("No instance of '%s' available" % self.__class__)
        if not args in ((),(None,)):
            coder = args[0]
        else:
            coder = kwargs.pop('coder', DEF_CODER) # None
        geocoder = cls.get_client(coder)
        coder = geocoder['coder']
        key = CODERS.get(coder)
        try:
            gc = getattr(geopy.geocoders, coder)
        except:
            try:
                get_gc = getattr(geopy.geocoders, 'get_geocoder_for_service')
            except:
                raise IOError("Coder not available")
            else:
                gc = get_gc(coder)
        if not key is None:
            kwargs.update({key: geocoder[key]})
        return gc(**kwargs)

    #/************************************************************************/
    @classmethod
    def geocoding(cls, query, coder = DEF_CODER, **kwargs):
        """Geocoding operator.

            >>> Service.geocoding(query, coder = DEF_CODER, **kwargs)

        Example
        -------

            >>> loc = Service.geocoding( "175 5th Avenue NYC",
                                         user_agent = 'Eurostat')
            >>> print('lat=%s, lon=%s' % (loc.latitude, loc.longitude))
                lat=40.741059199999995, lon=-73.98964162240998
        """
        return (cls.get_geoclient(coder = coder, **kwargs)
                .geocode(query))

    #TODO: consider using async mode
    # see https://geopy.readthedocs.io/en/stable/#async-mode
    # from geopy.adapters import AioHTTPAdapter
    # from geopy.geocoders import Nominatim

    # async with Nominatim(
    #     user_agent="specify_your_app_name_here",
    #     adapter_factory=AioHTTPAdapter,
    # ) as geolocator:
    #     location = await geolocator.geocode("175 5th Avenue NYC")
    #     print(location.address)

    #/************************************************************************/
    def __init__(self, *args,  **kwargs):
        try:
            assert _is_happy_installed is True or _is_geopy_installed is True
        except:
            raise ImportError("No instance of '%s' available" % self.__class__)
        if not args in ((),(None,)):
            coder = args[0]
        else:
            coder = kwargs.pop('coder', DEF_CODER) # None
        # exactly_one = kwargs.pop('exactly_one',None)
        self.agent = kwargs.pop('user_agent', DEF_AGENT)
        self.client = self.get_client(coder)
        coder = self.client['coder'].lower()
        try:
            assert _is_happy_installed is True
        except: # _is_geopy_installed is True and, hopefully, coder not in ('osm','GISCO')
            if coder in ('osm','gisco'):
                raise IOError('geocoder %s not available' % coder)
            # try:
            #     gc = getattr(geopy.geocoders, coder)
            # except:
            #     raise IOError("Coder not available")
            # else:
            #     kwargs.update({'user_agent': kwargs.pop('user_agent',DEF_AGENT)})
            #     if not key is None:
            #         kwargs.update({key: self.client[key]})
            #     self.geoclient = gc(**kwargs)
            kwargs.update({'user_agent': self.agent})
            self.geoclient = self.get_geoclient(coder = self.client, **kwargs)
        else:
            if coder == 'osm':
                self.geoclient = services.OSMService()
            elif coder == 'gisco':
                self.geoclient = services.GISCOService()
            else:
                self.geoclient = services.APIService(**self.client)
        self.crs, self.proj = None, None # no use

    #/************************************************************************/
    def __getattr__(self, attr):
        if attr in ('im_class','__objclass__'):
            return getattr(self.geoclient, '__class__')
        elif attr.startswith('__'):  # to avoid some bug of the pylint editor
            try:        return object.__getattribute__(self, attr)
            except:     pass
        try:        return getattr(self.geoclient, attr)
        except:     raise IOError("Attribute '%s' not available" % attr)

    #/************************************************************************/
    @property
    def agent(self):
        return self.__agent# or {}
    @agent.setter#analysis:ignore
    def agent(self, agent):
        if not (agent is None or isinstance(agent, string_types)):
            raise TypeError("Wrong format for geocoder user AGENT '%s' - must be a string" % agent)
        self.__agent = agent

    #/************************************************************************/
    @property
    def client(self):
        return self.__client# or {}
    @client.setter#analysis:ignore
    def client(self, client):
        if not (client is None or isinstance(client, Mapping)):
            raise TypeError("Wrong format for geocoder CLIENT '%s' - must be a dictionary" % client)
        self.__client = client

    #/************************************************************************/
    def locate(self, *place, **kwargs):
        """Geocoding method.

            >>> coord = geoserv.locate(*place, **kwargs)
        """
        try:
            assert _is_happy_installed is True or _is_geopy_installed is True
        except:
            raise ImportError("'locate' method not available")
        if 'place' in kwargs:
            place = (kwargs.pop('place', ''),)
        try:
            assert isinstance(place[0], (string_types,Sequence))
        except AssertionError:
            raise TypeError("Wrong type of PLACE columns name(s) '%s'" % place)
        kwargs.update({'order': 'lL', 'unique': True,
                      'exactly_one': True})
        if _is_happy_installed is True:
            if self.client['coder'] in ('osm','GISCO'):
                kwargs.pop('exactly_one')
            else:
                kwargs.pop('exactly_one')
            return self.geoclient.place2coord(place, **kwargs)
        # _is_geopy_installed is True
        kwargs.pop('unique', None) # just drop the key
        order = kwargs.pop('order', 'lL')
        try:
            loc = self.geoclient.geocode(place, **kwargs) # self.client._gc.geocode(place, **kwargs)
            lat, lon = loc.latitude, loc.longitude
        except:
            lat = lon = np.nan
        return [lat,lon] if order == 'lL' else [lon, lat]

    #/************************************************************************/
    def locate_apply(self, place):
        assert isinstance(place, string_types)
        try:
            loc = self.geoclient.geocode(place)
            return loc.latitude, loc.longitude
        except:
            return (np.nan, np.nan)

    #/************************************************************************/
    def project(self, *coord, **kwargs):
        """Projection method.

            >>> ncoord = geoserv.project(coord, iproj='WGS84', oproj='WGS84')
        """
        try:
            assert _is_happy_installed is True or _is_pyproj_installed is True
        except:
            raise ImportError("'project' method not available")
        if 'lat' in kwargs and 'lon' in kwargs:
            coord = ([kwargs.pop('lat', None), kwargs.pop('lon', None)],)
        if coord  in ((),(None,)):
            raise IOError("No 'lat/lon' coordinates parsed")
        elif not all([isinstance(c, Sequence) for c in coord]):
            raise TypeError("Wrong 'lat/lon' coordinates parsed")
        iproj = kwargs.pop('iproj', 'WGS84')
        if not isinstance(iproj, (string_types, int)):
            raise TypeError("Input projection '%s' not recognised - must be a string (e.g., 'WGS84' or 'EPSG:4326') or an integer (e.g., 4326)" % iproj)
        oproj = kwargs.pop('oproj', 'WGS84')
        if not isinstance(oproj, (string_types, int)):
            raise TypeError("Output projection '%s' not recognised - must be a string (e.g., 'WGS84' or 'EPSG:4326') or an integer (e.g., 4326)" % oproj)
        if iproj == oproj:
            return coord
        try:
            # assert _is_happy_installed is True
            return self.geoclient.coordproject(coord, iproj=iproj, oproj=oproj)
        except:
            try:
                # assert _is_pyproj_installed is True
                CRS = crs.from_epsg(oproj)
                return Transformer.from_crs(CRS.from_epsg(iproj), CRS).transform(*coord)
            except:
                raise IOError("Projection of coordinates failed...")

    #/************************************************************************/
    def project_apply(self, coord, iproj, oproj='WGS84'):
        assert isinstance(coord, Sequence)
        if iproj == oproj:
            return coord
        try:
            return self.geoclient.coordproject(coord, iproj=iproj, oproj=oproj)
        except:
            try:
                CRS = crs.from_epsg(oproj)
                return (Transformer
                        .from_crs(CRS.from_epsg(iproj), CRS)
                        .transform(*coord)
                        )
            except:
                return (np.nan, np.nan)


#==============================================================================
# Class Vector
#==============================================================================

class Vector(object):
    """Instantiation class for vector data.

        >>> vector = Vector()
    """

    #/************************************************************************/
    @staticmethod
    def open(file, src=None, **kwargs):
        """
        Accepts gdal compatible file remotely or on disk and returns gdal pointer.

        note:
            Considering the use of PushErrorHandler in open, this function
            prevents from being called inside another function.
        """
        # gdal.ErrorReset()
        # gdal.PushErrorHandler('QuietErrorHandler')
        # ogr.RegisterAll()
        try:
            assert file is None or isinstance(file, (bytes,io.BytesIO,io.StringIO,string_types))
        except:
            raise TypeError("Wrong type for file parameter - must be bytes or string")
        try:
            assert src is None or isinstance(src, string_types)
        except:
            raise TypeError("Wrong type for data source parameter - must be a string")
        if src is None:
            src, file = file, None
        driver = kwargs.pop('driver', None)
        try:
            assert isinstance(driver,string_types) and driver in DRIVERS
        except:
            raise TypeError("Wrong type for DRIVER parameter - must a GDAL driver")
        mode = kwargs.pop('mode', 0) # 0 means read-only. 1 means writeable.
        try:
            assert isinstance(mode,int) and mode in [0,1]
        except:
            raise TypeError("Wrong type for MODE parameter - must be 0 or 1")
        on_disk = kwargs.pop('on_disk',True)
        vsi = not on_disk and kwargs.pop('vsi',False)
        try:
            assert isinstance(on_disk,bool) and (isinstance(vsi,bool) or isinstance(vsi,string_types))
        except:
            raise TypeError("Wrong type for VIRTUAL and ON_DISK parameters - must be bool or string")
        if isinstance(src, (io.BytesIO,io.StringIO)):
            driver = "MEMORY"
        if driver is None:
            fopen = ogr.Open
        else:
            drv = ogr.GetDriverByName(driver)
            fopen = drv.Open
        if vsi is not False: # https://gdal.org/user/virtual_file_systems.html
            vname = vsi or Sys.uuid()
        if isinstance(src, (io.BytesIO,io.StringIO)):
            mmap = "/vsimem/%s" % vname
            try:
                gdal.FileFromMemBuffer(mmap, src.read())
            except:
                raise OSError("Error retrieving source data")
            else:
                src = mmap
        elif isinstance(src, bytes):
            mmap = "/vsimem/%s" % vname
            try:
                gdal.FileFromMemBuffer(mmap, src)
            except:
                raise OSError("Error retrieving source data")
            else:
                src = mmap
        elif isinstance(src,string_types):
            if not on_disk and zipfile.is_zipfile(src):
                if src.endswith('zip'):
                    mmap = "/vsizip"
                elif any([src.endswith(p) for p in ['gz', 'gzip']]):
                    mmap = "/vsizip"
                elif any([src.endswith(p) for p in ['tgz', 'tar']]):
                    mmap = "/vsitar"
                mmap = "%s/%ss/%s" % (mmap,src,file)
                try:
                    assert False
                    gdal.FileFromMemBuffer(mmap, src) #????
                except:
                    raise OSError("Error retrieving zipped source data")
                else:
                    src = mmap
            elif not FileSys.filexists(src):
                raise OSError("File '%s' not found on disk" % src)
            else:
                mmap = None # and src unchanged
        try:
            ds = fopen(src, update=mode)
            assert ds is not None
        except AssertionError:
            raise OSError("Nul source data")
        except:
            raise OSError("Error retrieving source data")
        # gdal.PopErrorHandler()
        if mmap not in (None,False):
            return ds, mmap
        else:
            return ds

    #/************************************************************************/
    @staticmethod
    def new(ds, **kwargs):
        try:
            assert isinstance(ds, string_types)
        except:
            raise TypeError("Wrong type for data source - must be a string")
        if FileSys.filexists(ds):
            FileSys.remove(ds)
        driver = kwargs.pop('driver', DEF_DRIVER)
        try:
            assert isinstance(driver,string_types) and driver in DRIVERS
        except:
            raise TypeError("Wrong type for DRIVER parameter - must a GDAL driver")
        else:
            driver = ogr.GetDriverByName(driver)
        try:
            return driver.CreateDataSource(ds)
        except:
            raise IOError("Error creating data source '%s'" % ds)

    #/************************************************************************/
    @staticmethod
    def open_layer(arg, **kwargs):
        try:
            assert isinstance(arg, (string_types,ogr.DataSource,ogr.Layer))
        except:
            raise TypeError("Wrong type for input parameter - must be a string, a data source or a layer")
        ds, layer = None, None
        if isinstance(arg, string_types):
            ds = Vector.open(arg, **kwargs)
            if isinstance(ds, Sequence):     ds = ds[0]
        elif isinstance(arg, ogr.DataSource):
            ds = arg
        elif isinstance(arg, ogr.Layer):
            return layer # dummy
        geom = kwargs.pop('geom', [])
        if geom is None:
            return ds.GetLayer()
        else:
            return ds.GetLayerByName(geom)

    #/************************************************************************/
    @staticmethod
    def spatialref4(proj):
        srs = osr.SpatialReference()
        try:
            assert srs.ImportFromProj4(proj) == 0
        except:
            raise IOError("Could not import proj4: %s'" % proj)
        else:
            return srs

    #/************************************************************************/
    @staticmethod
    def new_layer(arg, **kwargs):
        """Create layer from a datasource given proj4 and fields.
        """
        try:
            assert isinstance(arg, (string_types,ogr.DataSource,ogr.Layer))
        except:
            raise TypeError("Wrong type for input parameter - must be a string, a data source or a layer")
        if isinstance(arg, string_types):
            ds = Vector.new(arg, driver=kwargs.pop('driver',DEF_DRIVER))
            layer = osp.splitext(osp.basename(arg))[0] # osp.splitext(osp.split(arg)[1])[0]
        elif isinstance(arg, ogr.DataSource):
            ds = arg
        elif isinstance(arg,ogr.Layer):
            return arg
        # read other parameters
        srs = Vector.spatialref4(kwargs.pop('proj', None))
        layer, geom = kwargs.pop('layer',layer), kwargs.pop('geom',[])
        try:
            return ds.CreateLayer(layer, srs, geom)
        except:
            raise IOError("Could not create layer")

    #/************************************************************************/
    @staticmethod
    def read(arg, **kwargs):
        """Load proj4, shapegeo, fields.

        USAGE:
            shapegeo, proj, fieldpacks, fielddefs = read(layer, proj_src='', proj_dst='')
        """
        try:
            assert isinstance(arg, (string_types,ogr.DataSource,ogr.Layer))
        except:
            raise TypeError("Wrong type for input parameter - must be a string, a data source or a layer")
        # read layer
        layer = Vector.open_layer(arg)
        srs = layer.GetSpatialRef()
        # get spatialReference from the layer
        proj = srs.ExportToProj4() if srs else '' or kwargs.pop('proj','')
        kwargs.update({'proj': proj})
        # load features (shapegeo and fdpacks)
        fielddefs = Vector.read_field(layer)[0]
        geoms, fields = Vector.readlayer(layer, **kwargs)
        return geoms, proj, fields, fielddefs

    #/************************************************************************/
    @staticmethod
    def read_layer(layer, **kwargs):
        try:
            assert isinstance(layer, ogr.Layer)
        except:
            raise TypeError("Wrong type for input layer - must be a ogr.layer")
        iproj, oproj = kwargs.pop('iproj',''), kwargs.pop('oproj','')
        # get fddefs from featureDefinition
        fielddefs, fdindices = Vector.read_field(layer)
        feature = layer.GetNextFeature()
        geotf = Vector.geometry_factory(iproj, oproj)
        # loop over features
        geoms, fields = [], []
        while feature:
            geoms.append(wkb.loads(geotf(feature.GetGeometryRef()).ExportToWkb()))
            fields.append([feature.GetField(x) for x in fdindices])
            # get the next feature
            feature = layer.GetNextFeature()
        return geoms, fields #, fielddefs

    @staticmethod
    def geometry_factory(iproj, oproj=DEF_PROJ4LL):
        """Return a function that transforms a geometry from one spatial reference
        to another.
        """
        try:
            assert isinstance(iproj, string_types) and isinstance(oproj, string_types)
        except:
            raise TypeError("Wrong types for projections - must be strings")
        if iproj == oproj:
            return lambda g: g
        def geotrans(g, ct): # transform a shapelyGeometry or gdalGeometry
            is_base = isinstance(g, geometry.base.BaseGeometry) # test for shapelyGeometry
            if is_base: # if shapelyGeometry, convert to a gdalGeometry
                g = ogr.CreateGeometryFromWkb(g.wkb)
            try:
                assert g.Transform(ct) == 0
            except:
                raise IOError("Could not transform geometry: '%s'" % g.ExportToWkt())
            if is_base: # if we originally had a shapelyGeometry, convert it back
                g = wkb.loads(g.ExportToWkb())
            return g
        ct = osr.CoordinateTransformation(Vector.spatialref4(iproj),
                                          Vector.spatialref4(oproj))
        return lambda g: geotrans(g, ct)

    #/************************************************************************/
    @staticmethod
    def read_field(layer):
        featdef = layer.GetLayerDefn()
        fdindices = range(featdef.GetFieldCount())
        fielddefs = []
        for fdindex in fdindices:
            fielddef = featdef.GetFieldDefn(fdindex)
            fielddefs.append((fielddef.GetName(), fielddef.GetType()))
        return fielddefs, fdindices

    #/************************************************************************/
    @staticmethod
    def write(arg, **kwargs):
        """
        Save shapegeo using the given proj4 and fields in a given file, data
        source or layer.

        USAGE:
            write(path, shape=None, src='', dst='', fdpacks=None, fddefs=None, drv=DEF_VECTOR)
            write(ds, shape=None, src='', dst='', fdpacks=None, fddefs=None)
            write(layer, shape=None, src='', dst='', fdpacks=None, fddefs=None)
        """
        try:
            assert isinstance(arg, (string_types,ogr.DataSource,ogr.Layer))
        except:
            raise TypeError("Wrong type for input parameter - must be a string, a data source or a layer")
        iproj, oproj = kwargs.pop('iproj',''), kwargs.pop('oproj','')
        # any operation on projections ?
        geom = kwargs.pop('geom',[])
        # retrieve the layer
        layer = Vector.newl_ayer(arg, geom=Vector.geom2ogr(geom),
                                 proj=iproj or oproj)
        try:
            assert layer is not None
        except:
            raise IOError("Wrong layer")
        Vector.write_layer(layer, geom=geom, iproj=iproj, oproj=oproj)

    #/************************************************************************/
    @staticmethod
    def geom2ogr(geom): #
        """Determine OGR geometry type for layer.

        SYNTAX:
            ogrgeo = geom2ogr(geom)
        """
        geotypes = list(set(type(x) for x in geom))

        return ogr.wkbUnknown if len(geotypes) > 1 else {
            geometry.Point: ogr.wkbPoint,
            geometry.point.PointAdapter: ogr.wkbPoint,
            geometry.LineString: ogr.wkbLineString,
            geometry.linestring.LineStringAdapter: ogr.wkbLineString,
            geometry.Polygon: ogr.wkbPolygon,
            geometry.polygon.PolygonAdapter: ogr.wkbPolygon,
            geometry.MultiPoint: ogr.wkbMultiPoint,
            geometry.multipoint.MultiPointAdapter: ogr.wkbMultiPoint,
            geometry.MultiLineString: ogr.wkbMultiLineString,
            geometry.multilinestring.MultiLineStringAdapter: ogr.wkbMultiLineString,
            geometry.MultiPolygon: ogr.wkbMultiPolygon,
            geometry.multipolygon.MultiPolygonAdapter: ogr.wkbMultiPolygon,
        }[geotypes[0]]

    #/************************************************************************/
    @staticmethod
    def write_layer(layer, **kwargs):
        try:
            assert isinstance(layer, ogr.Layer)
        except:
            raise TypeError("Wrong type for input layer - must be an ogr.layer")
        # validate arguments
        fdpacks, fddefs = kwargs.pop('packs', []), kwargs.pop('defs', [])
        if fdpacks and set(len(x) for x in fdpacks) != set([len(fddefs)]):
            raise IOError("A field definition is required for each field")
        # make fddefs in featureDefinition
        [layer.CreateField(ogr.FieldDefn(fdname, fdtype))       \
             for fdname, fdtype in fddefs]
        featdef = layer.GetLayerDefn()
        geom = kwargs.pop('geom', [])
        geotf = Vector.geometry_factory(**kwargs)
        for shape, field in itertools.izip(geom, fdpacks)       \
                if fdpacks else ((x, []) for x in geom):
            featdef = layer.GetLayerDefn()  # prepare feature
            feature = ogr.Feature(featdef)
            feature.SetGeometry(geotf(ogr.CreateGeometryFromWkb(shape.wkb)))
            [feature.SetField(fdindex, fdvalue)  for fdindex, fdvalue in enumerate(field)]
            layer.CreateFeature(feature)    # save feature
            feature.Destroy()  # clean up

    #/************************************************************************/
    @staticmethod
    def write_field(field, **kwargs):
        """Create a field definition, which can be used to create a field using
        the CreateField() function. Simply create a "model" for a field, that can
        then be called later.
        """
        try:
            assert isinstance(field, string_types)
        except:
            raise TypeError("Wrong type for input field - must be an string")
        latlon = kwargs.pop('latlon',False)
        typ_ = kwargs.pop('type',ogr.OFTReal if latlon is True else ogr.OFTInteger)
        width = kwargs.pop('width', 12 if latlon is True else 10)
        prec = kwargs.pop('prec', 4 if latlon is True else 6)
        fieldDefn = ogr.FieldDefn(field, typ_)
        fieldDefn.SetWidth(width)
        fieldDefn.SetPrecision(prec)
        return fieldDefn


#==============================================================================
# Class Raster
#==============================================================================

class Raster(object):
    """Instantiation class for raster data.

        >>> raster = Raster()
    """
    pass
