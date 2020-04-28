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

import warnings#analysis:ignore

from collections import OrderedDict, Mapping, Sequence#analysis:ignore
from six import string_types

import numpy as np#analysis:ignore
import pandas as pd#analysis:ignore

try:
    assert False
    import happygisco#analysis:ignore
except (AssertionError,ImportError):
    _is_happy_installed = False
    warnings.warn('\n! Missing happygisco package (https://github.com/eurostat/happyGISCO) - GISCO web services not available !')
else:
    # warnings.warn('\n! happygisco help: hhttp://happygisco.readthedocs.io/ !')
    _is_happy_installed = True
    from happygisco import services
    # CODERS = {'GISCO':None, 'osm':None})

try:
    import geopy#analysis:ignore 
except ImportError: 
    _is_geopy_installed = False
    warnings.warn('\n! Missing geopy package (https://github.com/geopy/geopy) !')   
else:
    # warnings.warn('\n! geopy help: http://geopy.readthedocs.io/en/latest/ !')
    _is_geopy_installed = True
    # from geopy import geocoders
    #CODERS.update({'GoogleV3':'api_key', 'Bing':'api_key', 'GeoNames':'username', 
    #               'Yandex':'api_key', 'MapQuest':'key', 'Nominatim':None, 
    #               'OpenMapQuest':'api_key'})
    
try:
    assert _is_happy_installed is True or _is_geopy_installed is True
except AssertionError:
    # raise IOError('no geocoding module available')   
    warnings.warn('\n! No geocoding module available !')   
            
try:
    import pyproj#analysis:ignore 
except ImportError:
    #warnings.warn('\n! missing pyproj package (https://github.com/pyproj4/pyproj) !')
    _is_pyproj_installed = False
else:
    # warnings.warn('\n! pyproj help: https://pyproj4.github.io/pyproj/latest/ !')
    _is_pyproj_installed = True
    from pyproj import CRS as crs, Transformer

from pyeudatnat import COUNTRIES


#%% Global vars             

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
DEFCODER = {'Bing' : None} # 'GISCO', 'Nominatim', 'GoogleV3', 'GMaps', 'GPlace', 'GeoNames'

# LATLON        = ['lat', 'lon'] # 'coord' # 'latlon'
# ORDER         = 'lL' # first lat, second Lon 

PLACE = ['street', 'number', 'postcode', 'city', 'country']
"""Fields used to defined a toponomy (location/place).
"""


#==============================================================================
#%% Method isoCountry

def isoCountry(arg):
    """Given a country name or an ISO 3166 code, return the pair {name,code}.
    
        >>> country = isoCountry(country_or_cc)
            
    Examples
    --------
    
        >>> GeoProcess.isoCountry('CZ')
            {'code': 'CZ', 'country': 'Czechia'}
        >>> GeoProcess.isoCountry('Greece')
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
#%% Class GeoService
  
import time

class GeoService(object):
    """Instantiation class for geoprocessing module.
    
        >>> geoserv = GeoService()
    """
            
    #/************************************************************************/
    @classmethod
    def select_coder(cls, arg):
        """Define geocoder.
        
            >>> coder = GeoService.selectCoder(arg)
        """
        if arg is None:
            #arg = cls.DEFCODER.copy()
            raise IOError("No geocoder parsed")
        elif not isinstance(arg, (string_types,Mapping)):
            raise TypeError("Wrong format for geocoder '%s' - must be a string or a dictionary" % arg)
        elif isinstance(arg, string_types):
            coder, key = arg, None
        elif isinstance(arg, Mapping):
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
                coder, key = 'Bing', None
        return {'coder': coder, CODERS[coder]: key}
                        
    #/************************************************************************/
    def __init__(self, *args,  **kwargs):
        try:
            assert _is_happy_installed is True or _is_geopy_installed is True
        except:
            raise ImportError("No instance of '%s' available" % self.__class__)
        if not args in ((),(None,)):
            coder = args[0]
        else:       
            coder = kwargs.pop('coder', DEFCODER) # None
        self.geocoder = self.select_coder(coder) 
        coder = self.geocoder['coder']
        key = CODERS[coder]
        time.sleep(3)
        try:
            assert _is_happy_installed is True 
        except: # _is_geopy_installed is True and, hopefully, coder not in ('osm','GISCO')
            if coder.lower() in ('osm','gisco'): 
                raise IOError('geocoder %s not available' % coder)
            try:        
                gc = getattr(geopy.geocoders, coder)   
            except:     
                raise IOError("Coder not available")
            else:    
                self.geoserv = gc(**{key: self.geocoder[key]})            
        else:
            if coder.lower() == 'osm':  
                kwargs.pop('exactly_one')
                self.geoserv = services.OSMService()
            elif coder.lower() == 'gisco':  
                kwargs.pop('exactly_one')
                self.geoserv = services.GISCOService()
            else:
                kwargs.pop('exactly_one')
                self.geoserv = services.APIService(**self.geocoder)
        self.crs, self.proj = None, None # no use
    
    #/************************************************************************/
    def __getattr__(self, attr):
        if attr in ('im_class','__objclass__'): 
            return getattr(self.geoserv, '__class__')
        elif attr.startswith('__'):  # to avoid some bug of the pylint editor
            try:        return object.__getattribute__(self, attr) 
            except:     pass 
        try:        return getattr(self.geoserv, attr)
        except:     raise IOError("Attribute '%s' not available" % attr)

    #/************************************************************************/
    @property
    def geocoder(self):
        return self.__geocoder # or {}
    @geocoder.setter#analysis:ignore
    def geocoder(self, coder):
        if not (coder is None or isinstance(coder, Mapping)):         
            raise TypeError("Wrong format for geocoder '%s' - must be a string" % coder)
        self.__geocoder = coder

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
        kwargs.update({'order': 'lL', 'unique': True, 
                      'exactly_one': True})
        if _is_happy_installed is True:
            if self.geocoder['coder'] in ('osm','GISCO'): 
                kwargs.pop('exactly_one')
            else:
                kwargs.pop('exactly_one')
            return self.geoserv.place2coord(place, **kwargs)
        else: # _is_geopy_installed is True
            kwargs.pop('unique', None) # just drop the key
            order = kwargs.pop('order', 'lL')
            loc = self.geoserv.geocode(place, **kwargs) # self.geoserv._gc.geocode(place, **kwargs)
            lat, lon = loc.get('latitude'), loc.get('longitude')
            return [lat,lon] if order == 'lL' else [lon, lat] 
        
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
            return self.geoserv.coordproject(coord, iproj=iproj, oproj=oproj)
        except:
            try:
                # assert _is_pyproj_installed is True
                CRS = crs.from_epsg(oproj)
                return Transformer.from_crs(CRS.from_epsg(iproj), CRS).transform(*coord)
            except:
                raise IOError("Projection of coordinates failed...")

