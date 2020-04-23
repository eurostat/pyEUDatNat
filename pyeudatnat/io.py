#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.. _io

.. Links

.. _geojson: https://github.com/jazzband/geojson
.. |geojson| replace:: `geojson <geojson_>`_

Module implementing miscenalleous Input/Output methods.
    
**Dependencies**

*require*:      :mod:`os`, :mod:`six`, :mod:`collections`, :mod:`numpy`, :mod:`pandas`

*optional*:     :mod:`geojson`

**Contents**
"""

# *credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_ 
# *since*:        Thu Apr  9 09:56:45 2020

#%%                

import io, os#analysis:ignore
from os import path as osp
import warnings#analysis:ignore

from collections import OrderedDict, Mapping, Sequence#analysis:ignore
from six import string_types

import time
try: 
    from datetime import datetime
except ImportError:            
    pass 

import numpy as np#analysis:ignore
import pandas as pd#analysis:ignore

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
            def dump(arg):  
                return '%s' % arg
            def load(arg):  
                with open(arg,'r') as f:
                    return f.read()

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

from pyeudatnat.misc import Miscellaneous, File


#%%
#==============================================================================
# Class Requests
#==============================================================================
    
class Requests(object):

    #/************************************************************************/
    @staticmethod
    def default_cache():
        return File.cache()   

    #/************************************************************************/
    @staticmethod
    def build_cache(url, cache_store):
        pathname = url.encode('utf-8')
        try:
            pathname = hashlib.md5(pathname).hexdigest()
        except:
            pathname = pathname.hex()
        return osp.join(cache_store or './', pathname)

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

    #/************************************************************************/
    @staticmethod
    def cache_response(url, force, store, expire):
        # sequential implementation of cache_response
        pathname = Requests.build_cache(url, store)
        is_cached = Requests.is_cached(pathname, expire)
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
        force, store, expire = kwargs.pop('force', True), kwargs.pop('store', None), kwargs.pop('expire', 0)
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
    

#%%
#==============================================================================
# Class Dataframe
#==============================================================================
    
class Dataframe(object):
    """Static methods for Input/Output pandas dataframe processing, e.f. writing
    into a table.
    """

    #/************************************************************************/
    @staticmethod
    def out_date(df, column, ofmt=None, ifmt=None): # ofmt='%d/%m/%Y', ifmt='%d-%m-%Y %H:%M')
        """Cast the column of a dataframe into datetime.
        """
        try:
            assert column in df.columns
        except:
            raise IOError("Wrong input column - must be in the dataframe")
        try:
            assert (ofmt is None or isinstance(ofmt, string_types)) and     \
                isinstance(ifmt, string_types) 
        except:
            raise TypeError("Wrong format for input date templates")
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
    def out_cast(df, column, cast):
        """Cast the column of a dataframe into special format, excluding datetime.
        """
        try:
            assert column in df.columns
        except:
            raise IOError("Wrong input column - must be in the dataframe")
        try:
            assert isinstance(cast, type) is True
        except:
            raise TypeError("Wrong format for input cast type")
        if cast == df[column].dtype:
            return df[column]
        else:
            try:
                return df[column].astype(cast)
            except:
                return df[column].astype(object)
                
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
        """GEOsJSON output formatting.
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
    def to_gpkg(df, columns=None):
        """
        """
        warnings.warn('\n! method for gpkg not implemented !')
        pass
    
    #/************************************************************************/
    @staticmethod
    def to_xml(filename):
       pass
       
    #/************************************************************************/
    @staticmethod
    def to_file(df, dest, **kwargs):
        FMTS = ['csv', 'json', 'xls', 'xlsx', 'geojson']
        fmt = kwargs.pop('fmt', FMTS)
        encoding = kwargs.pop('encoding', None) or kwargs.pop('enc', None) or 'utf-8'
        if isinstance(fmt,string_types):
            fmt = [fmt,]
        try:
            assert isinstance(fmt, Sequence) and set(fmt).difference(set(FMTS))==set()
        except:
            raise IOError("Data format not recognised: '%s'" % fmt)
        try:
            assert 'csv' in fmt
            nkwargs = Miscellaneous.inspect_kwargs(kwargs, pd.to_csv)
            df.to_csv(dest, **nkwargs)
        except AssertionError:
            pass
        except:
            raise IOError("Impossible to write to CSV")
        else:
            if fmt == 'csv':    return
            else:               fmt.remove('csv')
        try:
            assert fmt is None or fmt in ('xls','xlsx')
            nkwargs = Miscellaneous.inspect_kwargs(kwargs, pd.to_excel)
            df.to_excel(dest, **nkwargs)
        except AssertionError:
            pass
        except:
            raise IOError("Impossible to write to Excel")
        else:
            if set(fmt).difference({'xls','xlsx'}) == set(): return
            else:   [fmt.remove(f) for f in ['xls','xlsx']]
        try:
            assert 'geojson' in fmt
            results = Dataframe.to_json(df, **nkwargs)
            with open(dest, 'w', encoding=encoding) as f:
                json.dump(results, f, ensure_ascii=False)
        except AssertionError:
            pass
        except:
            raise IOError("Impossible to write to GEOJSON")
        else:
            if fmt == 'geojson':    return
            else:               fmt.remove('geojson')
        try:
            assert 'json' in fmt
            results = Dataframe.to_json(df, **nkwargs)
            with open(dest, 'w', encoding=encoding) as f:
                json.dump(results, f, ensure_ascii=False)
        except AssertionError:
            pass
        except:
            raise IOError("Impossible to write to JSON")
        else:
            if fmt == 'json':    return
            else:               fmt.remove('json')

    #/************************************************************************/
    @staticmethod
    def from_html(htmlname, **kwargs):
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
        return pd.Dataframe(rows, columns = headers)
    
    #/************************************************************************/
    @staticmethod
    def from_xml(xmlname):
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
        FMTS = ['csv', 'json', 'xls', 'xlsx', 'xml', 'html']
        fmt = kwargs.pop('fmt', FMTS)
        all_fmt = kwargs.pop('all_fmt', False)
        try:    assert isinstance(all_fmt, bool) 
        except: raise IOError("Wrong format for ALL_FMT_ flag: '%s'" % all_fmt)
        if isinstance(fmt,string_types):
            fmt = [fmt,]
        try:    assert (isinstance(fmt, Sequence) and set(fmt).difference(set(FMTS))==set())
        except: raise IOError("Data format FMT not recognised: '%s'" % fmt)
        if all_fmt is True: # extend... with all besides those parsed
            fmt.extend(list(set(FMTS).difference(set(fmt)))) # test all!
        try:    
            fmt.insert(fmt.index('xlsx'),'xls') or fmt.remove('xlsx')
        except: pass
        fmt = [f for i, f in enumerate(fmt) if f not in fmt[:i]]# unique, ordered
        def read_csv(s, **kw):
            nkw = Miscellaneous.inspect_kwargs(kw, pd.read_csv)
            return pd.read_csv(s, **nkw)
        def read_excel(s, **kw):
            nkw = Miscellaneous.inspect_kwargs(kw, pd.read_excel)
            return pd.read_excel(s, **nkw)
        def read_json(s, **kw):
            nkw = Miscellaneous.inspect_kwargs(kw, pd.read_json)
            return pd.read_json(s, **nkw)
        def read_html(s, **kw):
            nkw = Miscellaneous.inspect_kwargs(kw, pd.read_html)
            return pd.read_html(s, **nkw)
        def from_xml(s, **kw):
            return Dataframe.from_xml(s, **kw)
        def read_table(s, **kw):
            nkw = Miscellaneous.inspect_kwargs(kw, pd.read_table)
            return pd.read_table(s, **nkw)
        funloads = {'csv':      read_csv, 
                    'xls':      read_excel, 
                    'json':     read_json, 
                    'html':     read_html, 
                    'xml':      from_xml, 
                    'table':    read_table}
        for f in fmt:
            try:
                df = funloads[f](src, **kwargs)
            except FileNotFoundError:            
                raise IOError("Impossible to load source data - file '%s' not found" % src)
            except:
                pass
            else:
                # warnings.warn("\n! %s data loaded in dataframe !" % f.upper())
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
            return Dataframe.from_data(data **kwargs)
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
        #    return Dataframe.from_data(data, **kwargs)
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
            _namelist = [osp.basename(n) for n in namelist]
            if members is None and len(namelist)==1:
                members = namelist
            elif members is not None:
                for i in reversed(range(len(members))):
                    m = members[i]
                    try:
                        assert m in namelist
                    except:
                        try:    assert m in _namelist
                        except: members.pop(i)
                        else:   members[i] = namelist[_namelist.index(m)]
                    else:
                        pass
            if members in ([],None):
                raise IOError("Impossible to retrieve member file(s) from zipped data")
            for m in members:
                try:
                    with zf.open(m) as zm:
                        df = Dataframe.from_data(zm, **kwargs)
                except:
                    raise IOError("Data %s cannot be read in source file... abort!" % m)
                else:
                    results.update({m: df})
        return results if len(results.keys())>1 else list(results.values())[0]
    
    #/************************************************************************/
    @staticmethod
    def from_source(src, file=None, **kwargs):
        """
        """
        try:
            assert src is None or isinstance(src, string_types)
        except:
            raise TypeError("Wrong type for data source parameter '%s' - must be a string" % src)
        try:
            assert file is None or isinstance(file, string_types)
        except:
            raise TypeError("Wrong type for file parameter '%s' - must be a string" % file)
        if src is None:
            src, file = file, None
        if any([src.startswith(p) for p in ['http', 'https', 'ftp'] ]):
            try:
                data = Requests.read_url(src, **kwargs)
            except:
                raise IOError("Wrong request for data source from URL '%s'" % src) 
        else:
            try:
                assert File.file_exists(src) is True
            except:
                raise IOError("Data source '%s' not found on disk" % src)  
            else:
                data = src
        ## option 1: transforming files in zipped source directly into dataframe 
        ## while unziping with from_zip
        #if zipfile.is_zipfile(data) or any([src.endswith(p) for p in ['zip', 'gz', 'gzip', 'bz2'] ]):
        #    try:
        #        return Dataframe.from_zip(data, file, **kwargs)
        #    except:
        #        raise IOError("Impossible unzipping data from zipped file '%s'" % src)   
        #else:
        #    try:    fmt = os.path.splitext(file)[-1].replace('.','')
        #    except: pass
        #    else:   kwargs.update({'fmt':fmt, 'all_fmt': False})
        #    try:
        #        return Dataframe.from_data(data, **kwargs)
        #    except:
        #        raise IOError("Wrong formatting of source data into dataframe")             
        # option 2: opening and parsing files from zipped source to transform
        # them into dataframes 
        if zipfile.is_zipfile(data) or any([src.endswith(p) for p in ['zip', 'gz', 'gzip', 'bz2'] ]):
            try:
                # file = File.unzip(data, namelist=True) 
                kwargs.update({'open': file}) # when file=None, will read a single file
                data, file = File.unzip(data, **kwargs) 
            except:
                raise IOError("Impossible unzipping data from zipped file '%s'" % src)   
        try:
            fmt = os.path.splitext(file)[-1].replace('.','')
        except:
            pass
        else:
            kwargs.update({'fmt':fmt, 'all_fmt': False})
        try:
            return Dataframe.from_data(data, **kwargs)
        except:
            raise IOError("Wrong formatting of source data into dataframe") 
        

#%%
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
        nkwargs = Miscellaneous.inspect_kwargs(kwargs, json.dump)
        try:        assert serialize is True 
        except:     json.dump(data, f, **nkwargs)
        else:       json.dump(cls.serialize(data), f, **nkwargs)
    
    @classmethod
    def dumps(cls, data, **kwargs):
        serialize = kwargs.pop('serialize', False)    
        nkwargs = Miscellaneous.inspect_kwargs(kwargs, json.dumps)
        try:        assert serialize is True 
        except:     return json.dumps(data, **nkwargs)
        else:       return json.dumps(cls.serialize(data), **nkwargs)
    
    @classmethod
    def load(cls, s, **kwargs):
        serialize = kwargs.pop('serialize', False)
        nkwargs = Miscellaneous.inspect_kwargs(kwargs, json.load)
        try:        assert serialize is True 
        except:     return json.load(s, **nkwargs)
        else:       return json.load(s, object_hook=cls.restore, **nkwargs)

    @classmethod
    def loads(cls, s, **kwargs):
        serialize = kwargs.pop('serialize', False)
        nkwargs = Miscellaneous.inspect_kwargs(kwargs, json.loads)
        try:        assert serialize is True 
        except:     return json.loads(s, **kwargs)
        else:       return json.loads(s, object_hook=cls.restore, **nkwargs)
