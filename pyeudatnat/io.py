#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.. _io

.. Links

.. _geojson: https://github.com/jazzband/geojson
.. |geojson| replace:: `geojson <geojson_>`_
.. _bs4: https://pypi.python.org/pypi/beautifulsoup4
.. |bs4| replace:: `beautiful soup <bs4_>`_
.. _chardet: https://pypi.org/project/chardet/
.. |chardet| replace:: `chardet <chardet_>`_
.. _xmltree: https://docs.python.org/3/library/xml.etree.elementtree.html
.. |xmltree| replace:: `xml.tree <xmltree_>`_

Module implementing miscenalleous Input/Output methods.

**Dependencies**

*require*:      :mod:`os`, :mod:`six`, :mod:`collections`, :mod:`numpy`, :mod:`pandas`,
                :mod:`time`, :mod:`requests`, :mod:`hashlib`, :mod:`shutil`

*optional*:     :mod:`simplejson`, :mod:`json`, :mod:`geojson`, :mod:`zipfile`, :mod:`bs4`,
                :mod:`datetime`, :mod:`chardet`, :mod:`xml.etree`

*call*:         :mod:`pyeudatnat.misc`

**Contents**
"""

# *credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_
# *since*:        Thu Apr  9 09:56:45 2020

#%% Settings

import io, os, sys
from os import path as osp
import warnings#analysis:ignore

from collections import OrderedDict
from collections.abc import Mapping, Sequence
from six import string_types

import time
try:
    from datetime import datetime
except ImportError:
    pass

import numpy as np
import pandas as pd

try:
    import geopandas as gpd
except:
    _is_geopandas_installed = False
    class gpd():
        @staticmethod
        def notinstalled(meth):
            raise IOError("Package GeoPandas not installed = method '%s' not available" % meth)
        def to_file(self,*args, **kwargs):      self.notinstalled('to_file')
        def read_file(self,*args, **kwargs):    self.notinstalled('read_file')
else:
    _is_geopandas_installed = True

import requests # urllib2
import hashlib
import shutil

try:
    import simplejson as json
except ImportError:
    try:
        import json
    except ImportError:
        class json:
            def dump(f, arg):
                with open(arg,'w') as f:    f.write(arg)
            def dumps(arg):                 return '%s' % arg
            def load(arg):
                with open(arg,'r') as f:    return f.read()
            def loads(arg):                 return '%s' % arg

try:
    import zipfile
except:
    _is_zipfile_installed = False
else:
    _is_zipfile_installed = True

# Beautiful soup package
try:
    import bs4
except ImportError:
    # warnings.warn("missing beautifulsoup4 module - visit https://pypi.python.org/pypi/beautifulsoup4", ImportWarning)
    _is_bs4_installed = False
else:
    _is_bs4_installed = True

try:
    import chardet
except ImportError:
    #warnings.warn('\n! missing chardet package (visit https://pypi.org/project/chardet/ !')
    pass

try:
    import geojson#analysis:ignore
except ImportError:
    #warnings.warn('\n! missing geosjon package (https://github.com/jazzband/geojson) !')
    _is_geojson_installed = False
else:
    #warnings.warn('\n! geojson help: https://github.com/jazzband/geojson !')
    _is_geojson_installed = True
    from geojson import Feature, Point, FeatureCollection

try:
    import xml.etree.cElementTree as et
except ImportError:
    _is_xml_installed = False
else:
    _is_xml_installed = True

from pyeudatnat import PACKNAME
from pyeudatnat.misc import Object, Structure


FORMATS         = { 'csv':          'csv',
                    'json':         'json',
                    'excel':        ['xls', 'xlsx'],
                    'table':        'table',
                    'xml':          'xml',
                    'html':         'html',
                    'sql':          'sql',
                    'sas':          'sas',
                    'geojson':      'geojson',
                    'topojson':     'topojson',
                    'shapefile':    'shp',
                    'geopackage':   'gpkg',
                    'htmltab':      'htmltab'
                    }
DEFFORMAT       = 'csv'
# See Pandas supported IO formats: https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html
# See also GDAL supported drivers (or fiona.supported_drivers)


ENCODINGS       = { 'utf-8':       'utf-8',
                    'latin':       'ISO-8859-1',
                    'ISO-8859-1':  'ISO-8859-1',
                    'latin1':      'ISO-8859-2',
                    'ISO-8859-2':  'ISO-8859-2'
                    }
DEFENCODING     = 'latin'

DEFSEP          = ';'

#%% Core functions/classes

#==============================================================================
# Class File
#==============================================================================

class File(object):

    #/************************************************************************/
    @staticmethod
    def is_writable(file):
        """Determine if a temporary file can be written in the same directory as a
        given file.

            >>> resp = File.is_writable(file)
        """
        if not osp.isdir(file):
            file = osp.dirname(file)
        try:
            from tempfile import TemporaryFile
            tmp = TemporaryFile(dir=file) # can we write a temp file there...?
        except: return False
        else:
            del tmp
            return True

    #/************************************************************************/
    @staticmethod
    def check_format(fmt, infer_fmt=False):
        try:
            assert fmt is None or isinstance(fmt, string_types)  \
                or (isinstance(fmt, Sequence) and all([isinstance(f, string_types) for f in fmt]))
        except: raise IOError("Wrong format for FMT parameter: '%s'" % fmt)
        if fmt is None:                             fmt = list(FORMATS.values())
        elif isinstance(fmt, string_types):         fmt = [fmt,]
        try:
            assert isinstance(infer_fmt, bool) or isinstance(infer_fmt, string_types) \
                or (isinstance(infer_fmt, Sequence) and all([isinstance(f, string_types) for f in infer_fmt]))
        except:
            raise IOError("Wrong format for INFER_FMT flag: '%s'" % infer_fmt)
        if infer_fmt is True: # extend... with all besides those parsed
            infer_fmt = FORMATS.keys()   # default
        elif isinstance(infer_fmt, string_types):
                infer_fmt = [infer_fmt,]
        if not infer_fmt is False: # extend... with all besides those parsed
            fmt.extend(infer_fmt) # test all!
        try:
            fmt.insert(fmt.index('xlsx'),'xls') or fmt.remove('xlsx')
        except: pass
        fmt = Structure.uniq_items(fmt, items=FORMATS)
        try:
            assert fmt not in (None,[],'')
        except:
            raise IOError("Data format FMT not recognised: '%s'" % fmt)
        if isinstance(fmt,string_types):
            fmt = [fmt,]
        return fmt

    #/************************************************************************/
    @staticmethod
    def default_cache():
        platform = sys.platform
        if platform.startswith("win"): # windows
            basedir = os.getenv("LOCALAPPDATA",os.getenv("APPDATA",osp.expanduser("~")))
        elif platform.startswith("darwin"): # Mac OS
            basedir = osp.expanduser("~/Library/Caches")
        else:
            basedir = os.getenv("XDG_CACHE_HOME",osp.expanduser("~/.cache"))
        return osp.join(basedir, PACKNAME)

    #/************************************************************************/
    @staticmethod
    def build_cache(path, cache_store=None):
        if cache_store in (None,''):
            cache_store = './'
        # elif cache_store in (None,'default'):
        #    cache_store = File.default_cache()
        pathname = path.encode('utf-8')
        try:
            pathname = hashlib.md5(pathname).hexdigest()
        except:
            pathname = pathname.hex()
        return osp.join(cache_store, pathname)

    #/************************************************************************/
    @staticmethod
    def is_cached(pathname, time_out): # note: we check a path here
        if not osp.exists(pathname):
            resp = False
        elif time_out is None:
            resp = True
        elif time_out < 0:
            resp = True
        elif time_out == 0:
            resp = False
        else:
            cur = time.time()
            mtime = os.stat(pathname).st_mtime
            warnings.warn("\n! %s - last modified: %s !" % (pathname,time.ctime(mtime)))
            resp = cur - mtime < time_out
        return resp

    #/************************************************************************/
    @staticmethod
    def clean_cache(pathname, time_expiration): # note: we clean a path here
        if not osp.exists(pathname):
            resp = False
        elif time_expiration is None or time_expiration <= 0:
            resp = True
        else:
            cur = time.time()
            mtime = os.stat(pathname).st_mtime
            warnings.warn("\n! '%s' - last modified: %s !" % (pathname,time.ctime(mtime)))
            resp = cur - mtime >= time_expiration
        if resp is True:
            warnings.warn("\n! Removing disk file %s !" % pathname)
            if osp.isfile(pathname):
                os.remove(pathname)
            elif osp.isdir(pathname):
                shutil.rmtree(pathname)

    #/****************************************************************************/
    @staticmethod
    def pick_lines(file, lines):
        """Pick numbered lines from file.
        """
        return [x for i, x in enumerate(file) if i in lines]

    #/************************************************************************/
    @staticmethod
    def unzip(file, **kwargs):
        # try:
        #     assert isinstance(file, (io.BytesIO,string_types))
        # except:
        #     raise TypeError("Zip file '%s' not recognised" % file)
        try:
            assert zipfile.is_zipfile(file)
        except:
            raise IOError("Zip file '%s' not recognised" % file)
        path = kwargs.pop('path') if 'path' in kwargs else File.default_cache()
        operators = [op for op in ['open', 'extract', 'extractall', 'getinfo', 'namelist', 'read', 'infolist'] \
                     if op in kwargs.keys()]
        try:
            assert operators in ([],[None]) or sum([1 for op in operators]) == 1
        except:
            raise IOError("Only one operation supported per call")
        else:
            if operators in ([],[None]):
                operator = 'extractall'
            else:
                operator = operators[0]
        if operator in ('infolist','namelist'):
            try:
                assert kwargs.get(operator) not in (False,None)
            except:
                raise IOError("No operation parsed")
        else:
            members = kwargs.pop(operator, None)
        #if operator.startswith('extract'):
        #    warnings.warn("\n! Data extracted from zip file will be physically stored on local disk !")
        if isinstance(members,string_types):
            members = [members,]
        with zipfile.ZipFile(file) as zf:
            namelist, infolist = zf.namelist(), zf.infolist()
            if operator == 'namelist':
                return namelist if len(namelist)>1 else namelist[0]
            elif operator == 'infolist':
                return infolist if len(infolist)>1 else infolist[0]
            elif operator == 'extractall':
                if members in (None,True):  members = namelist
                return zf.extractall(path = path, members = members)
            if members is None and len(namelist)==1:
                members = namelist
            elif members is not None:
                for i in reversed(range(len(members))):
                    m = members[i]
                    try:
                        assert m in namelist
                    except:
                        try:
                            _mem = [n for n in namelist if n.endswith(m)]
                            assert len(_mem)==1
                        except:
                            if len(_mem) > 1:
                                warnings.warn("\n! Mulitple files machting in zip source - ambiguity not resolved !" % m)
                            else: # len(_mem) == 0 <=> _mem = []
                                warnings.warn("\n! File '%s' not found in zip source !" % m)
                            members.pop(i)
                        else:
                            members[i] = _mem[0]
                    else:
                        pass # continue
            # now: operator in ('extract', 'getinfo', 'read')
            if members in ([],None):
                raise IOError("Impossible to retrieve member file(s) from zipped data")
            nkw = Object.inspect_kwargs(kwargs, getattr(zf, operator))
            if operator == 'extract':
                nkw.update({'path': path})
            results = {m: getattr(zf, operator)(m, **nkw) for m in members}
        return results
        # raise IOError("Operation '%s' failed" % operator)


#==============================================================================
# Class Requests
#==============================================================================

class Requests(object):

    #/************************************************************************/
    @staticmethod
    def cache_response(url, force, store, expire):
        # sequential implementation of cache_response
        pathname = File.build_cache(url, store)
        is_cached = File.is_cached(pathname, expire)
        if force is True or is_cached is False or store in (None,False):
            response = requests.get(url)
            content = response.content
            if store not in (None,False):
                # write "content" to a given pathname
                with open(pathname, 'wb') as f:
                    f.write(content)
        else:
            # read "content" from a given pathname.
            with open(pathname, 'rb') as f:
                content = f.read()
        return content, pathname

    #/************************************************************************/
    @staticmethod
    def get_response(url, caching=False, force=True, store=None, expire=0):
        if caching is False or store is None:
            try:
                response = requests.get(url)
                response.raise_for_status()
            except: # (requests.URLRequired,requests.HTTPError,requests.RequestException):
                raise IOError("Wrong request formulated")
        else:
            try:
                response, _ = Requests.cache_response(url, force, store, expire)
                response.raise_for_status()
            except:
                raise IOError("Wrong request formulated")
        try:
            assert response is not None
        except:
            raise IOError("Wrong response retrieved")
        return response

    #/************************************************************************/
    @staticmethod
    def read_response(response, stream=None):
        if stream is None:
            try:
                url = response.url
            except:
                stream = 'json'
            else:
                stream = 'zip' if any([url.endswith(z) for z in ('zip','gzip','gz')]) else 'json'
        if stream in ('resp','response'):
            return response
        try:
            assert isinstance(stream,string_types)
        except:
            raise TypeError("Wrong format for STREAM parameter")
        else:
            stream = stream.lower()
        try:
            assert stream in ['jsontext', 'jsonbytes', 'resp', 'zip', 'raw', 'content',
                              'text', 'stringio', 'bytes', 'bytesio', 'json']
        except:
            raise IOError("Wrong value for STREAM parameter")
        else:
            if stream == 'content':
                stream = 'bytes'
        if stream.startswith('json'):
            try:
                assert stream not in ('jsontext', 'jsonbytes')
                data = response.json()
            except:
                try:
                    assert stream != 'jsonbytes'
                    data = response.text
                except:
                    try:
                        data = response.content
                    except:
                        raise IOError("Error JSON-encoding of response")
                    else:
                        stream = 'jsonbytes' # force
                else:
                    stream = 'jsontext' # force
            else:
                return data
        elif stream == 'raw':
            try:
                data = response.raw
            except:
                raise IOError("Error accessing ''raw'' attribute of response")
        elif stream in ('text', 'stringio'):
            try:
                data = response.text
            except:
                raise IOError("Error accessing ''text'' attribute of response")
        elif stream in ('bytes', 'bytesio', 'zip'):
            try:
                data = response.content
            except:
                raise IOError("Error accessing ''content'' attribute of response")
        if stream == 'stringio':
            try:
                data = io.StringIO(data)
            except:
                raise IOError("Error loading StringIO data")
        elif stream in ('bytesio', 'zip'):
            try:
                data = io.BytesIO(data)
            except:
                raise IOError("Error loading BytesIO data")
        elif stream == 'jsontext':
            try:
                data = json.loads(data)
            except:
                raise IOError("Error JSON-encoding of str text")
        elif stream == 'jsonbytes':
                try:
                    data = json.loads(data.decode())
                except:
                    try:
                         # assert _is_chardet_installed is True
                        data = json.loads(data.decode(chardet.detect(data)["encoding"]))
                    except:
                        raise IOError("Error JSON-encoding of bytes content")
        return data

    #/************************************************************************/
    @staticmethod
    def read_url(urlname, **kwargs):
        stream = kwargs.pop('stream', None)
        caching = kwargs.pop('caching', False)
        force, store, expire = kwargs.pop('cache_force', True), kwargs.pop('cache_store', None), kwargs.pop('cache_expire', 0)
        try:
            assert any([urlname.startswith(p) for p in ['http', 'https', 'ftp']]) is True
        except:
            #raise IOError ?
            warnings.warn("\n! Protocol not encoded in URL !")
        try:
            response = Requests.get_response(urlname, caching=caching, force=force,
                                             store=store, expire=expire)
        except:
            raise IOError("Wrong request for data from URL '%s'" % urlname)
        try:
            data = Requests.read_response(response, stream=stream)
        except:
            raise IOError("Impossible reading data from URL '%s'" % urlname)
        return data


#==============================================================================
# Class Buffer
#==============================================================================

class Buffer(object):

    #/************************************************************************/
    @staticmethod
    def from_url(urlname, **kwargs): # dumb function
        try:
            return Requests.read_url(urlname, **kwargs)
        except:
            raise IOError("Wrong request for data from URL '%s'" % urlname)

    #/************************************************************************/
    @staticmethod
    def from_zip(file, src=None, **kwargs): # dumb function
        try:
            assert file is None or isinstance(file, string_types)
        except:
            raise TypeError("Wrong type for file parameter '%s' - must be a string" % file)
        try:
            assert src is None or isinstance(src, string_types)
        except:
            raise TypeError("Wrong type for data source parameter '%s' - must be a string" % src)
        if src is None:
            src, file = file, None
        if zipfile.is_zipfile(src) or any([src.endswith(p) for p in ['zip', 'gz', 'gzip', 'bz2'] ]):
            try:
                # file = File.unzip(content, namelist=True)
                kwargs.update({'open': file}) # when file=None, will read a single file
                results = File.unzip(src, **kwargs)
            except:
                raise IOError("Impossible unzipping content from zipped file '%s'" % src)
        else:
            results = {src: file}
        return results if len(results.keys())>1 else list(results.values())[0]

    #/************************************************************************/
    @staticmethod
    def from_vector(name, **kwargs):
        warnings.warn("\n! Method 'from_vector' for geographical vector data loading not implemented !'")
        pass

    #/************************************************************************/
    @staticmethod
    def from_file(file, src=None, **kwargs):
        """
        """
        try:
            assert file is None or isinstance(file, string_types)     \
                 or (isinstance(file, Sequence) and all([isinstance(f,string_types) for f in file]))
        except:
             raise TypeError("Wrong format for filename - must be a (list of) string(s)")
        try:
            assert src is None or isinstance(src, string_types)
        except:
            raise TypeError("Wrong type for data source parameter '%s' - must be a string" % src)
        if src is None:
            src, file = file, None
        if any([src.startswith(p) for p in ['http', 'https', 'ftp'] ]):
            try:
                content = Requests.read_url(src, **kwargs)
            except:
                raise IOError("Wrong request for data source from URL '%s'" % src)
        else:
            try:
                assert osp.exists(src) is True
            except:
                raise IOError("Data source '%s' not found on disk" % src)
            else:
                content = src
        # opening and parsing files from zipped source to transform
        # # them into dataframes -
        if kwargs.get('on_disk',False) is True:
            # path = kwargs.pop('store') if 'store' in kwargs else File.default_cache()
            path = kwargs.pop('store',None) or File.default_cache()
            kwargs.update({'extract': file, 'path': path})
        else:
            kwargs.update({'open': file}) # when file=None, will read a single file
        if zipfile.is_zipfile(content) or any([src.endswith(p) for p in ['zip', 'gz', 'gzip', 'bz2'] ]):
            try:
                # file = File.unzip(content, namelist=True)
                results = File.unzip(content, **kwargs)
            except:
                raise IOError("Impossible unzipping content from zipped file '%s'" % src)
        else:
            results = {file: content}
        # with 'extract', the normalised path to the file is returned
        #if kwargs.get('on_disk',False) is True:
        #    [results.update({f: osp.join(p,f)}) for f,p in results.items()]
        return results if len(results.keys())>1 else list(results.values())[0]


#==============================================================================
# Class Frame
#==============================================================================

class Frame(object):
    """Static methods for Input/Output pandas dataframe processing, e.f. writing
    into a table.
    """

    #/************************************************************************/
    @staticmethod
    def cast(df, column, otype=None, ofmt=None, ifmt=None):
        """Cast the column of a dataframe into special type or date format.

            >>> dfnew = Frame.cast(df, column, otype=None, ofmt=None, ifmt=None)
        """
        try:
            assert column in df.columns
        except:
            raise IOError("Wrong input column - must be in the dataframe")
        try:
            assert otype is None or (ofmt is None and ifmt is None)
        except:
            raise IOError("Incompatible option OTYPE with IFMT and OFMT")
        try:
            assert (otype is None or isinstance(otype, type) is True)
        except:
            raise TypeError("Wrong format for input cast type")
        try:
            assert (ofmt is None or isinstance(ofmt, string_types))     \
                and (ifmt is None or isinstance(ifmt, string_types))
        except:
            raise TypeError("Wrong format for input date templates")
        if otype is not None:
            if otype == df[column].dtype:
                return df[column]
            else:
                try:
                    return df[column].astype(otype)
                except:
                    return df[column].astype(object)
        else:
             # ofmt='%d/%m/%Y', ifmt='%d-%m-%Y %H:%M'
            if ifmt in (None,'') :
                kwargs = {'infer_datetime_format': True}
            else:
                kwargs = {}
            if ofmt in (None,'') or ofmt == '':
                return df[column].astype(str)
            else:
                try:
                    f = lambda s: datetime.strptime(s, ifmt, **kwargs).strftime(ofmt)
                    return df[column].astype(str).apply(f)
                except:
                    return df[column].astype(str)

    #/************************************************************************/
    @staticmethod
    def to_json(df, columns=None):
        """JSON output formatting.
        """
        try:
            assert columns is None or isinstance(columns, string_types)     or \
                (isinstance(columns, Sequence) and all([isinstance(c,string_types) for c in columns]))
        except:
            raise IOError("Wrong format for input columns")
        if isinstance(columns, string_types):
            columns == [columns,]
        if columns in (None,[]):
            columns = df.columns
        columns = list(set(columns).intersection(df.columns))
        df.reindex(columns = columns)
        return df[columns].to_dict('records')

    #/************************************************************************/
    @staticmethod
    def to_geojson(df, columns=None, latlon=['lat', 'lon']):
        """GEOJSON output formatting.
        """
        try:
            assert columns is None or isinstance(columns, string_types)     or \
                (isinstance(columns, Sequence) and all([isinstance(c,string_types) for c in columns]))
        except:
            raise IOError("Wrong format for input columns")
        try:
            lat, lon = latlon
            assert isinstance(lat, string_types) and isinstance(lon, string_types)
        except:
            raise TypeError("Wrong format for input lat/lon columns")
        if isinstance(columns, string_types):
            columns == [columns,]
        if columns in (None,[]):
            columns = list(set(df.columns))
        columns = list(set(columns).intersection(set(df.columns)).difference(set([lat,lon])))
        # df.reindex(columns = columns) # not necessary
        if _is_geojson_installed is True:
            features = df.apply(
                    lambda row: Feature(geometry=Point((float(row[lon]), float(row[lat])))),
                    axis=1).tolist()
            properties = df[columns].to_dict('records') # columns used as properties
            # properties = df.drop([lat, lon], axis=1).to_dict('records')
            geom = FeatureCollection(features=features, properties=properties)
        else:
            geom = {'type':'FeatureCollection', 'features':[]}
            for _, row in df.iterrows():
                feature = {'type':'Feature',
                           'properties':{},
                           'geometry':{'type':'Point',
                                       'coordinates':[]}}
                feature['geometry']['coordinates'] = [float(row[lon]), float(row[lat])]
                for col in columns:
                    feature['properties'][col] = row[col]
                geom['features'].append(feature)
        return geom

    #/************************************************************************/
    @staticmethod
    def to_xml(filename):
        """
        """
        warnings.warn("\n! Method 'to_xml' for xml data writing not implemented !")
        pass

    #/************************************************************************/
    @staticmethod
    def to_file(df, dest, **kwargs):
        ofmt = kwargs.pop('fmt', None)
        infer_fmt = kwargs.pop('infer_fmt', False)
        if infer_fmt is True:
            infer_fmt = ['csv', 'json', 'excel', 'geojson', 'xls'] # list(FORMATS.values())
        try:
            ofmt = File.check_format(ofmt, infer_fmt = infer_fmt)
        except:
            raise IOError("Data format FMT not recognised: '%s'" % ofmt)
        encoding = kwargs.pop('encoding', None) or kwargs.pop('enc', None) or 'utf-8'
        def _to_csv(df, d, **kw):
            nkw = Object.inspect_kwargs(kw, pd.to_csv)
            df.to_csv(d, **nkw)
        def _to_excel(df, d, **kw):
            nkw = Object.inspect_kwargs(kwargs, pd.to_excel)
            df.to_excel(d, **nkw)
        def _to_json(df, d, **kw):
            nkw = Object.inspect_kwargs(kwargs, Frame.to_json)
            res = Frame.to_json(df, **nkw)
            with open(d, 'w', encoding=encoding) as f:
                json.dump(res, f, ensure_ascii=False)
        def _to_geojson(df, d, **kw):
            if _is_geojson_installed is True:
                nkw = Object.inspect_kwargs(kwargs, gpd.to_file)
                df.to_file(d, driver='GeoJSON', **nkw)
            else:
                nkw = Object.inspect_kwargs(kwargs, Frame.to_json)
                res = Frame.to_json(df, **nkw)
                with open(d, 'w', encoding=encoding) as f:
                    json.dump(res, f, ensure_ascii=False)
        def _to_geopackage(df, d, **kw):
            nkw = Object.inspect_kwargs(kwargs, gpd.to_file)
            df.to_file(d, driver='GPKG', **nkw)
        fundumps = {'csv':      _to_csv,
                    'xls':      _to_excel,
                    'json':     _to_json,
                    'geojson':  _to_geojson,
                    'gpkg':     _to_geopackage
                    }
        for f in ofmt:
            try:
                assert not osp.exists(dest)
            except:
                warnings.warn("\n! Output file '%s' already exist - will be overwritten")
            try:
                fundumps[f](dest, **kwargs)
            except:
                warnings.warn("\n! Impossible to write to %s !" % f.upper())
            else:
                if ofmt == f:       return
                else:               ofmt.remove(f)
        raise IOError("Impossible to save data - input format not recognised")

    #/************************************************************************/
    @staticmethod
    def from_html_table(htmlname, **kwargs):
        """
        """
        try:
            assert _is_bs4_installed is True
        except:
            raise IOError("'from_html' method not available")
        parser = kwargs.get('kwargs','html.parser')
        if parser not in ('html.parser','html5lib','lxml'):
            raise IOError("Unknown soup parser")
        try:
            raw = bs4.BeautifulSoup(htmlname, parser)
            #raw = bs4.BeautifulSoup(html, parser).get_text()
        except:
            raise IOError("Impossible to read HTML page")
        try:
            tables = raw.findAll('table', **kwargs)
        except:
            raise IOError("Error with soup from HTML page")
        headers, rows = [], []
        for table in tables:
            try:
                table_body = table.find('tbody') # may be None
                headers.append(table_body.find_all('th'))
                rows.append(table_body.find_all('tr'))
            except:
                headers.append(table.findAll('th'))
                rows.append(table.findAll('tr'))
        return pd.DataFrame(rows, columns = headers)

    #/************************************************************************/
    @staticmethod
    def from_xml_tree(xmlname, **kwargs):
        """
        """
        try:
            assert _is_xml_installed is True
        except:
            raise IOError("'from_xml' method not available")
        #root = et.XML(filename) # element tree
        #records = []
        #for i, child in enumerate(root):
        #    record = {}
        #    for subchild in child:
        #        record[subchild.tag] = subchild.text
        #    records.append(record)
        #return pd.DataFrame(records)
        def iter_records(records):
            for record in records:
                dtmp = {}   # temporary dictionary to hold values
                for var in record: # iterate through all the fields
                   dtmp.update({var.attrib['var_name']: var.text})
                yield dtmp # generate the value
        with open(xmlname, 'r') as fx:
            tree = et.parse(fx) # read the data and store it as a tree
            root = tree.getroot() # get the root of the tree
            return pd.DataFrame(list(iter_records(root)))

    #/************************************************************************/
    @staticmethod
    def from_data(src, **kwargs):
        """
        """
        ifmt = kwargs.pop('fmt', None)
        infer_fmt= kwargs.pop('infer_fmt', False)
        if infer_fmt is True:
            # note that the default infer_fmt here may differ from the one in
            # from_file method
            infer_fmt = ['csv', 'json', 'excel', 'sql', 'html', 'table']
        try:
            ifmt = File.check_format(ifmt, infer_fmt = infer_fmt)
        except:
            raise IOError("Data format FMT not recognised: '%s'" % ifmt)
        def _read_csv(s, **kw):
            nkw = Object.inspect_kwargs(kw, pd.read_csv)
            return pd.read_csv(s, **nkw)
        def _read_excel(s, **kw):
            nkw = Object.inspect_kwargs(kw, pd.read_excel)
            return pd.read_excel(s, **nkw)
        def _read_json(s, **kw):
            nkw = Object.inspect_kwargs(kw, pd.read_json)
            return pd.read_json(s, **nkw)
        def _read_sql(s, **kw):
            nkw = Object.inspect_kwargs(kw, pd.read_sql)
            return pd.read_sql(s, **nkw)
        def _read_sas(s, **kw):
            nkw = Object.inspect_kwargs(kw, pd.read_sas)
            return pd.read_sas(s, **nkw)
        def _read_geojson(s, **kw):
            if _is_geopandas_installed is True:
                nkw = Object.inspect_kwargs(kw, gpd.read_file)
                nkw.update({'driver': 'GeoJSON'})
                return gpd.read_file(s, **nkw)
            else:
                nkw = Object.inspect_kwargs(kw, geojson.load)
                # note that geojson.load is a wrapper around the core json.load function
                # with the same name, and will pass through any additional arguments
                return geojson.load(s, **nkw)
        def _read_topojson(s, **kw):
            nkw = Object.inspect_kwargs(kw, gpd.read_file)
            nkw.update({'driver': 'TopoJSON'})
            #with fiona.MemoryFile(s) as f:  #with fiona.ZipMemoryFile(s) as f:
            #    return gpd.GeoDataFrame.from_features(f, crs=f.crs, **nkw)
            return gpd.read_file(s, **nkw)
        def _read_shapefile(s, **kw):
            try:
                assert osp.exists(s) is True # Misc.File.file_exists(s)
            except:
                warnings.warn("\n! GeoPandas reads URLs and files on disk only - set flags on_disk=True and ignore_buffer=True when loading sourc")
            try:
                p, f = osp.dirname(s), osp.basename(os.path.splitext(s)[0])
            except:
                pass
            try:
                assert (osp.exists(osp.join(p,'%s.shx' % f)) or osp.exists(osp.join(p,'%s.SHX' % f)))   \
                    and (osp.exists(osp.join(p,'%s.prj' % f)) or osp.exists(osp.join(p,'%s.PRJ' % f)))  \
                    and (osp.exists(osp.join(p,'%s.dbf' % f)) or osp.exists(osp.join(p,'%s.DBF' % f)))
            except AssertionError:
                warnings.warn("\n! Companion files [.dbf, .shx, .prj] are required together with shapefile source"
                              " - add companion files to path, e.g. set flags fmt='csv' and 'infer_fmt'=False when loading source")
            except:
                pass
            nkw = Object.inspect_kwargs(kw, gpd.read_file)
            nkw.update({'driver': 'shapefile'})
            return gpd.read_file(s, **nkw)
        def _read_geopackage(s, **kw):
            nkw = Object.inspect_kwargs(kw, gpd.read_file)
            nkw.update({'driver': 'GPKG'})
            return gpd.read_file(s, **nkw)
        def _read_html(s, **kw):
            nkw = Object.inspect_kwargs(kw, pd.read_html)
            return pd.read_html(s, **nkw)
        def _read_htmltab(s, **kw):
            return Frame.from_html_table(s, **kw)
        def _read_xml(s, **kw):
            return Frame.from_xml(s, **kw)
        def _read_table(s, **kw):
            nkw = Object.inspect_kwargs(kw, pd.read_table)
            return pd.read_table(s, **nkw)
        funloads = {'csv':      _read_csv,
                    'xls':      _read_excel,
                    'json':     _read_json,
                    'sql':      _read_sql,
                    'sas':      _read_sas,
                    'geojson':  _read_geojson,
                    'topojson': _read_topojson,
                    'shp':      _read_shapefile,
                    'gpkg':     _read_geopackage,
                    'html':     _read_html,
                    'htmltab':  _read_htmltab,
                    'xml':      _read_xml,
                    'table':    _read_table,
                    }
        for f in ifmt:
            try:
                df = funloads[f](src, **kwargs)
            except FileNotFoundError:
                raise IOError("Impossible to load source data - file '%s' not found" % src)
            except:
                pass
            else:
                # warnings.warn("\n! '%s' data loaded in dataframe !" % f.upper())
                return df
        raise IOError("Impossible to load source data - format not recognised")

    #/************************************************************************/
    @staticmethod
    def from_url(urlname, **kwargs):
        """
        """
        try:
            data = Requests.read_url(urlname, **kwargs)
        except:
            raise IOError("Wrong request for data from URL '%s'" % urlname)
        try:
            return Frame.from_data(data **kwargs)
        except:
            raise IOError("Wrong formatting of online data into dataframe")

    #/************************************************************************/
    @staticmethod
    def from_zip(file, members, **kwargs):
        """
        """
        try:
            assert zipfile.is_zipfile(file)
        except:
            raise TypeError("Zip file '%s' not recognised" % file)
        #kwargs.update({'read': kwargs.pop('file', None)})
        #try:
        #    data = File.unzip(file, **kwargs) # when None, and single file, read it
        #except:
        #    raise IOError("Impossible unzipping data from zipped file '%s'" % file)
        #try:
        #    return Frame.from_data(data, **kwargs)
        #except:
        #    raise IOError("Wrong formatting of zipped data into dataframe")
        try:
            assert members is None or isinstance(members,string_types) \
                or (isinstance(members,Sequence) and all([isinstance(m,string_types) for m in members]))
        except:
            raise TypeError("Wrong member '%s' not recognised" % members)
        else:
            if isinstance(members,string_types):
                members = [members,]
        results = {}
        with zipfile.ZipFile(file) as zf:
            namelist = zf.namelist()
            if members is None and len(namelist)==1:
                members = namelist
            elif members is not None:
                for i in reversed(range(len(members))):
                    m = members[i]
                    try:
                        assert m in namelist
                    except:
                        try:
                            _mem = [n for n in namelist if n.endswith(m)]
                            assert len(_mem)==1
                        except:
                            members.pop(i)
                        else:
                            members[i] = _mem[0]
                    else:
                        pass # continue
            if members in ([],None):
                raise IOError("Impossible to retrieve member file(s) from zipped data")
            for m in members:
                try:
                    with zf.open(m) as zm:
                        df = Frame.from_data(zm, **kwargs)
                except:
                    raise IOError("Data %s cannot be read in source file... abort!" % m)
                else:
                    results.update({m: df})
        return results if len(results.keys())>1 else list(results.values())[0]

    #/************************************************************************/
    @staticmethod
    def from_file(file, src=None, **kwargs):
        """
        Keyword arguments
        -----------------
        on_disk : bool
        stream : 'str'
        infer_fmt : bool
        fmt : str
        """
        ifmt = kwargs.pop('fmt', None)
        infer_fmt = kwargs.pop('infer_fmt', False)
        # stream = kwargs.pop('stream', None)'
        # on_disk = kwargs.pop('on_disk', False)'
        if infer_fmt is True:
            # note that the default infer_fmt here may differ from the one
            # in from_data method
            infer_fmt = ['csv', 'json', 'excel', 'html', 'geojson', 'shapefile', 'table'] # list(FORMATS.values())
        try:
            ifmt = File.check_format(ifmt, infer_fmt = infer_fmt)
        except:
            raise IOError("Data format FMT not recognised: '%s'" % ifmt)
        else:
            infer_fmt = False # update to avoid doing it again in from_data method
        # try:
        #     assert src is None or isinstance(src, string_types)
        # except:
        #     raise TypeError("Wrong type for data source parameter '%s' - must be a string" % src)
        # try:
        #     assert file is None or isinstance(file, string_types)
        # except:
        #     raise TypeError("Wrong type for file parameter '%s' - must be a string" % file)
        # if src is None:
        #     src, file = file, None
        # if any([src.startswith(p) for p in ['http', 'https', 'ftp'] ]):
        #     try:
        #         data = Requests.read_url(src, **kwargs)
        #     except:
        #         raise IOError("Wrong request for data source from URL '%s'" % src)
        # else:
        #     try:
        #         assert osp.exists(src) is True
        #     except:
        #         raise IOError("Data source '%s' not found on disk" % src)
        #     else:
        #         data = src
        ## transforming files in zipped source directly into dataframe while
        ## unziping with from_zip
        # if zipfile.is_zipfile(data) or any([src.endswith(p) for p in ['zip', 'gz', 'gzip', 'bz2'] ]):
        #     try:
        #         return Frame.from_zip(data, file, **kwargs)
        #     except:
        #         raise IOError("Impossible unzipping data from zipped file '%s'" % src)
        # else:
        #     try:    fmt = os.path.splitext(src)[-1].replace('.','')
        #     except: pass
        #     else:   kwargs.update({'fmt':fmt, 'infer_fmt': False})
        #     try:
        #         return Frame.from_data(data, **kwargs)
        #     except:
        #         raise IOError("Wrong formatting of source data into dataframe")
        # fetching opening and parsing files from source to transform them into
        # dataframes
        buffer = Buffer.from_file(file, src=src, **kwargs)
        if isinstance(buffer,string_types) or not isinstance(buffer,Mapping):
            buffer = {file: buffer}
        results = {}
        for file, data in buffer.items():
            ext = os.path.splitext(file)[-1].replace('.','').lower()
            if not(ifmt is None or ext in ifmt):
                warnings.warn("\n! File '%s' will not be loaded !" % file)
                continue
            else:
                fmt = ext
            kwargs.update({'fmt': fmt, 'infer_fmt': infer_fmt})
            try:
                results.update({file: Frame.from_data(data, **kwargs)})
                # not that this will work for zipfile but it is not generic, contraty
                # to the one above:
                #  results.update({file: Frame.from_data(io.BytesIO(data.read()), **kwargs)})
            except:
                raise IOError("Wrong formatting of source data into dataframe")
        return results if len(results.keys())>1 else list(results.values())[0]


#==============================================================================
# Class Series
#==============================================================================

class Series(Frame):
    pass


#==============================================================================
# Class Json
#==============================================================================

class Json(object):

    @classmethod
    def serialize(cls, data):
        if data is None or isinstance(data, (type, bool, int, float, str)):
            return data
        elif isinstance(data, Sequence):
            if isinstance(data, list):          return [cls.serialize(val) for val in data]
            elif isinstance(data, tuple):       return {"tup": [cls.serialize(val) for val in data]}
        elif isinstance(data, Mapping):
            if isinstance(data, OrderedDict):   return {"odic": [[cls.serialize(k), cls.serialize(v)] for k, v in data.items()]}
            elif isinstance(data, dict):
                if all(isinstance(k, str) for k in data):
                    return {k: cls.serialize(v) for k, v in data.items()}
                return {"dic": [[cls.serialize(k), cls.serialize(v)] for k, v in data.items()]}
        elif isinstance(data, set):             return {"set": [cls.serialize(val) for val in data]}
        raise TypeError("Type %s not data-serializable" % type(data))

    @classmethod
    def restore(cls, dct):
        if "dic" in dct:            return dict(dct["dic"])
        elif "tup" in dct:          return tuple(dct["tup"])
        elif "set" in dct:          return set(dct["set"])
        elif "odic" in dct:         return OrderedDict(dct["odic"])
        return dct

    @classmethod
    def dump(cls, data, f, **kwargs):
        serialize = kwargs.pop('serialize', False)
        # note: when is_order_preserved is False, this entire class can actually be
        # ignored since the dump/load methods are exactly equivalent to the original
        # dump/load method of the json package
        nkwargs = Object.inspect_kwargs(kwargs, json.dump)
        try:        assert serialize is True
        except:     json.dump(data, f, **nkwargs)
        else:       json.dump(cls.serialize(data), f, **nkwargs)

    @classmethod
    def dumps(cls, data, **kwargs):
        serialize = kwargs.pop('serialize', False)
        nkwargs = Object.inspect_kwargs(kwargs, json.dumps)
        try:        assert serialize is True
        except:     return json.dumps(data, **nkwargs)
        else:       return json.dumps(cls.serialize(data), **nkwargs)

    @classmethod
    def load(cls, s, **kwargs):
        serialize = kwargs.pop('serialize', False)
        nkwargs = Object.inspect_kwargs(kwargs, json.load)
        try:        assert serialize is True
        except:     return json.load(s, **nkwargs)
        else:       return json.load(s, object_hook=cls.restore, **nkwargs)

    @classmethod
    def loads(cls, s, **kwargs):
        serialize = kwargs.pop('serialize', False)
        nkwargs = Object.inspect_kwargs(kwargs, json.loads)
        try:        assert serialize is True
        except:     return json.loads(s, **kwargs)
        else:       return json.loads(s, object_hook=cls.restore, **nkwargs)
