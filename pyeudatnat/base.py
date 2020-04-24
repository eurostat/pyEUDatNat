#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.. _base

.. Links

.. _Eurostat: http://ec.europa.eu/eurostat/web/main
.. |Eurostat| replace:: `Eurostat <Eurostat_>`_

Base module enabling the harvesting and ingestion of data collected at country/region/local 
level into harmonised format.

**Dependencies**

*require*:      :mod:`os`, :mod:`six`, :mod:`collections`, :mod:`functools`, :mod:`copy`, 
                :mod`datetime`, :mod:`numpy`, :mod:`pandas`

*optional*:     :mod:`requests`, :mod:`simplejson`

*call*:         :mod:`pyeudatnat`, :mod:`pyeudatnat.meta`, :mod:`pyeudatnat.io`, :mod:`pyeudatnat.text`, :mod:`pyeudatnat.geo`           

**Contents**
"""

# *credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_ 
# *since*:        Tue Fri 20 23:14:24 2020

#%%                

from os import path as osp
import warnings#analysis:ignore

from collections import Mapping, Sequence
from six import string_types

from datetime import datetime, timedelta
from copy import deepcopy

import numpy as np#analysis:ignore
import pandas as pd

from pyeudatnat import PACKPATH, COUNTRIES
from pyeudatnat.meta import MetaDat, MetaDatNat
from pyeudatnat.io import Json, Dataframe
from pyeudatnat.text import LANGS, Interpret, isoLang
from pyeudatnat.geo import GeoService, isoCountry

__THISDIR         = osp.dirname(__file__)

_INDEX_ALWAYS_AS_ILANG = True # that's actually a debug...that is not a debug anymore!
_META_ALWAYS_UPDATED = False

#%%                

BASETYPE        = {t.__name__: t for t in [type, bool, int, float, str, datetime]}
    
    
#%%
#==============================================================================
# Class BaseDatNat
#==============================================================================

class BaseDatNat(object):
    """Base class used to represent national data sources.
    
        >>> dat = BaseDatNat(**metadata)
    """
    
    CATEGORY = None
    COUNTRY = None # class attribute... that should not be different from cc
    YEAR = None # for future...
    
    #/************************************************************************/
    def __init__(self, *args, **kwargs):
        # self.__config, self.__metadata = {}, {}
        self.__data = None                # data
        self.__columns, self.__index = {}, []
        self.__place, self.__proj = '', None
        try:
            # meta should be initialised in the derived class
            assert self.__metadata not in ({},None)
        except(AttributeError,AssertionError):
            if not args in ((),(None,)):        
                self.meta = MetaDat(args[0])
            else:
                #warnings.warn("\n! No metadata parsed !")
                self.meta = MetaDat()   
        try:
            # config should be initialised in the derived class
            assert self.__config not in ({},None)
        except (AttributeError,AssertionError):
            #warnings.warn("\n! No configuration parsed !")
            self.config = MetaDat()
        # retrieve type
        self.category = kwargs.pop('category', 
                                   self.config.category if hasattr(self.config, 'category') else self.CATEGORY)
        # retrieve country name and code
        country = isoCountry(self.meta.get('country') or self.COUNTRY)
        self.cc = kwargs.pop('cc', None if country in ({},None) else country.get('code'))
        # retrieve languge of the input data
        lang = isoLang(self.meta.get('lang'))
        self.lang = kwargs.pop('lang', None if lang in ({},None) else lang.get('code'))
        # retrieve input data parameters, e.g. name, location, and format 
        self.enc = kwargs.pop('enc',self.meta.get('enc'))
        self.sep = kwargs.pop('sep',self.meta.get('sep'))        
        path, fname = kwargs.pop('path',self.meta.get('path') or ''), kwargs.pop('file',self.meta.get('file') or '')
        if osp.basename(fname) != fname:
            path, fname = osp.abspath(osp.join(path, osp.dirname(fname))), osp.basename(fname)
        self.file = None if fname==''                           \
            else osp.join(path, fname) if path not in (None,'') \
            else fname
        self.source = kwargs.pop('source', self.meta.get('source') or None)
        # retrieve caching arguments
        self.cache = {'caching':kwargs.pop('caching',False), 'store':None, 'expire':0, 'force':True}
        if self.cache['caching'] is True:
            self.cache.update({'store': kwargs.pop('store', ''),
                               'expire': kwargs.pop('expire', 0),
                               'force': kwargs.pop('force',False)
                              })
        # retrieve data year, if any
        self.year = kwargs.pop('year', None)
        # retrieve a default output name
        self.dest = kwargs.pop('dest', None)
        # retrieve the input data projection
        self.proj = kwargs.pop('proj', self.meta.get('proj')) # projection system
        # retrieve a default output name
        self.date = kwargs.pop('date', self.meta.get('date') or '%d-%m-%Y %H:%M') # input date format
        # retrieve columns when already known
        columns = kwargs.pop('columns', None) 
        self.columns = columns or self.meta.get('columns') or []    # header columns
        [col.update({self.lang: col.get(self.lang) or ''}) for col in self.columns] # ensure there are 'locale' column names
        # retrieve matching columns when known
        index = kwargs.pop('index', None)   # index
        self.index = index or self.meta.get('index') or {}

    ##/************************************************************************/
    #def __repr__(self):
    #    return "<{} data instance at {}>".format(self.__class__.__name__, id(self))
    #def __str__(self):    
    #    keys = ['cc', 'country', 'file']
    #    l = max([len(k) for k in keys])
    #    return reduce(lambda x,y:x+y, ["{} : {}\n".format(k.ljust(l),getattr(self,k))
    #        for k in keys if self.get(k) not in ('',None)])    

    #/************************************************************************/
    def __getattr__(self, attr):
        if attr.startswith('meta_'):
        # if not object.__getattribute__(self,'__metadata') in (None,{}) and attr.startswith('meta_'):
            try:        return self.meta.get(attr[len('meta_'):]) 
            except:     pass
        elif attr.startswith('cfg_'):
        #elif not object.__getattribute__(self,'__config') in (None,{}) and attr.startswith('cfg_'):
            try:        return self.config.get(attr[len('cfg_'):]) 
            except:     pass
        else:
            pass
        try:        return object.__getattribute__(self, attr) 
        except AttributeError: 
            try:
                # assert attr in self.meta
                return object.__getattribute__(self, '__' + attr)
            except (AttributeError,AssertionError): 
                try:
                    assert False
                    # return getattr(self.__class__, attr)
                except AssertionError:
                    raise AttributeError("%s object has no attribute '%s'" % (type(self),attr))

    #/************************************************************************/
    @property
    def meta(self):
        return self.__metadata # or {}
    @meta.setter#analysis:ignore
    def meta(self, meta):
        if not (meta is None or isinstance(meta, (MetaDatNat,Mapping))):         
            raise TypeError("Wrong format for country METAdata '%s' - must be a dictionary" % meta)
        self.__metadata = meta

    @property
    def config(self):
        return self.__config # or {}
    @config.setter#analysis:ignore
    def config(self, cfg):
        if not (cfg is None or isinstance(cfg, (MetaDat,Mapping))):         
            raise TypeError("Wrong format for CONFIGuration info '%s' - must be a dictionary" % cfg)
        self.__config = cfg

    @property
    def category(self):
        return self.__category # or ''
    @category.setter#analysis:ignore
    def category(self, cat):
        if cat is None or isinstance(cat, (string_types, Mapping)):                          
            pass
        #elif isinstance(cat, Mapping):
        #    cat = str(list(typ.values())[0])
        else:
            raise TypeError("Wrong format for CATEGORY '%s' - must be a string (or a dictionary)" % cat)
        self.__category = cat

    @property
    def cc(self):
        return self.__cc
    @cc.setter#analysis:ignore
    def cc(self, cc):
        if cc is None:                          pass
        elif not isinstance(cc, string_types):         
            raise TypeError("Wrong format for CC country code '%s' - must be a string" % cc)
        elif not cc in COUNTRIES: # COUNTRIES.keys()
            raise IOError("Wrong CC country code '%s' - must be any valid ISO code from the EU area" % cc)   
        elif cc != next(iter(self.COUNTRY)):
            warnings.warn("\n! Mismatch with class variable 'CC': %s !" % next(iter(self.COUNTRY)))
        if _META_ALWAYS_UPDATED is True and cc is not None:
            self.meta.update({'country': {'code': cc, 'name': COUNTRIES[cc]}}) # isoCountry
        self.__cc = cc

    @property
    def country(self):
        return COUNTRIES[self.cc]

    @property
    def lang(self):
        return self.__lang
    @lang.setter#analysis:ignore
    def lang(self, lang):
        if lang is None:                          pass
        elif not isinstance(lang, string_types):         
            raise TypeError("Wrong format for LANGuage type '%s' - must be a string" % lang)
        elif not lang in LANGS: # LANGS.keys()
            raise IOError("Wrong LANGuage '%s' - must be any valid ISO language code" % lang)   
        if _META_ALWAYS_UPDATED is True and lang not in (None,{}):
            self.meta.update({'lang': {'code': lang, 'name': LANGS[lang]}}) # isoLang
        self.__lang = lang

    @property
    def year(self):
        return self.__refdate
    @year.setter#analysis:ignore
    def year(self, year):
        if not (year is None or isinstance(year, int)):         
            raise TypeError("Wrong format for YEAR: '%s' - must be an integer" % year)
        if _META_ALWAYS_UPDATED is True and year is not None:
            self.meta.update({'year': year})
        self.__refdate = year

    @property
    def source(self):
        return self.__source
    @source.setter#analysis:ignore
    def source(self, src):
        if not (src is None or isinstance(src, string_types)):         
            raise TypeError("Wrong format for data SOURCE '%s' - must be a string" % src)
        if _META_ALWAYS_UPDATED is True and src not in (None,''):
            self.meta.update({'source': src})
        self.__source = src

    @property
    def file(self):
        return self.__file
    @file.setter#analysis:ignore
    def file(self, file):
        if not (file is None or isinstance(file, string_types)):         
            raise TypeError("Wrong format for source FILE '%s' - must be a string" % file)
        if _META_ALWAYS_UPDATED is True and file is not None:
            self.meta.update({'file': None if file is None else osp.basename(file), 
                              'path': None if file is None else osp.dirname(file)})
        self.__file = file
        
    #/************************************************************************/
    @property
    def cache(self):
        return self.__cache
    @cache.setter
    def cache(self, cache):
        if cache is None:
            cache = {}
        elif not isinstance(cache, Mapping):
            raise TypeError("Wrong type for CACHE parameter - must be a dictionary")
        elif set(cache.keys()).difference({'caching','store','expire','force'}) != set():
            raise IOError("Keys for CACHE dictionary not recognised")
        if not(cache.get('caching') is None or isinstance(cache['caching'], (str,bool))):
            raise TypeError("Wrong type for CACHING flag")
        elif not(cache.get('store') is None or isinstance(cache['store'], str)):
            raise TypeError("Wrong type for STORE parameter")
        elif not(cache.get('expire') is None or     \
                 (isinstance(cache['expire'], (int, timedelta))) and int(cache['expire'])>=-1):
            raise TypeError("Wrong type for EXPIRE parameter")
        elif not(cache.get('force') is None or isinstance(cache['force'], bool)):
            raise TypeError("Wrong type for FORCE flag")
        self.__cache = cache

    @property
    def proj(self):
        return self.__proj
    @proj.setter#analysis:ignore
    def proj(self, proj):
        if not (proj is None or isinstance(proj, string_types)):         
            raise TypeError("Wrong format for PROJection type '%s' - must be a string" % proj)
        if _META_ALWAYS_UPDATED is True and proj is not None:
            self.meta.update({'proj': proj})
        self.__proj = proj

    @property
    def columns(self):
        return self.__columns  # self.meta.get('columns')
    @columns.setter#analysis:ignore
    def columns(self, cols):
        if cols is None:                        
            pass # nothing yet
        elif isinstance(cols, string_types):
            cols = [{self.lang: cols}]
        elif isinstance(cols, Mapping):
            cols = [cols,]
        elif isinstance(cols, Sequence) and all([isinstance(col, string_types) for col in cols]):
            cols = [{self.lang: col} for col in cols]
        elif not(isinstance(cols, Sequence) and all([isinstance(col, Mapping) for col in cols])): 
            raise TypeError("Wrong Input COLUMNS headers type '%s' - must be a sequence of dictionaries" % cols)
        if _META_ALWAYS_UPDATED is True and cols not in (None,[]):
            self.meta.update({'columns': cols})
        self.__columns = cols

    @property
    def index(self):
        return self.__index  # self.meta.get('index')
    @index.setter#analysis:ignore
    def index(self, ind):
        if ind is None:                        
            pass # nothing yet
        elif isinstance(ind, string_types):
            ind = {ind: None}
        elif isinstance(ind, Sequence):
            ind = dict.fromkeys(ind)
        elif not isinstance(ind, Mapping):
            raise TypeError("Wrong Output INDEX type '%s' - must be a dictionary" % ind)
        if _META_ALWAYS_UPDATED is True and ind not in ({},None):
            self.meta.update({'index': ind})
        self.__index = ind

    @property
    def sep(self):
        return self.__sep
    @sep.setter#analysis:ignore
    def sep(self, sep):
        if not (sep is None or isinstance(sep, string_types)):         
            raise TypeError("Wrong format for SEParator '%s' - must be a string" % sep)
        if _META_ALWAYS_UPDATED is True and sep not in ('',None):
            self.meta.update({'sep': sep})
        self.__sep = sep

    @property
    def enc(self):
        return self.__encoding
    @enc.setter#analysis:ignore
    def enc(self, enc):
        if not (enc is None or isinstance(enc, string_types)):         
            raise TypeError("Wrong format for file ENCoding '%s' - must be a string" % enc)
        if _META_ALWAYS_UPDATED is True and enc is not None:
            self.meta.update({'enc': enc})
        self.__encoding = enc

    @property
    def place(self):
        return self.__place
    @place.setter#analysis:ignore
    def place(self, place):
        if place is None:                        
            pass # nothing yet
        elif isinstance(place, string_types):
            pass # place = [place,]
        elif not(isinstance(place, Sequence) and all([isinstance(p, string_types) for p in place])):
            raise TypeError("Wrong input format for PLACE '%s' - must be a (list of) string(s) or a mapping dictionary" % place)            
        self.__place = place

    #/************************************************************************/
    def load_data(self, *src, **kwargs):
        """Load data source file.
        
                >>> fac.load_data('data')
        """
        src = (src not in ((None,),()) and src[0]) or kwargs.pop('source', None) or self.source                                               
        file = kwargs.pop('file', None) or self.file 
        if src in (None,'') and file in (None,''):     
             raise IOError("No source filename provided - set keyword file attribute/parameter")
        elif not(src is None or isinstance(src, string_types)):     
             raise TypeError('wrong format for source data - must be a string')
        elif not(file is None or isinstance(file, string_types)):     
             raise TypeError("Wrong format for filename - must be a string")
        # ifmt = osp.splitext(src)[-1]
        encoding = kwargs.pop('enc', self.enc) # self.meta.get('enc')
        sep = kwargs.pop('sep', self.sep) # self.meta.get('sep')
        kwargs.update({'dtype': object, 'encoding': encoding, 'sep': sep, 
                       'compression': 'infer'})
        kwargs.update(self.cache)
        self.data = Dataframe.from_source(src, file=file, **kwargs)       
        try:
            assert self.columns not in (None,[],[{}])
        except: 
            self.columns = [{self.lang:col} for col in self.data.columns]
        #if set([col[self.lang] for col in self.columns]).difference(set(self.data.columns)) != set():
        #    warnings.warn('\n! mismatched data columns and header fields !')
        # if everything worked well, update the fields in case they differ
        if self.source != src:             self.source = src
        if self.file != file:           self.file = file
        if self.enc != encoding:        self.enc = encoding 
        if self.sep != sep:             self.sep = sep 
        
    #/************************************************************************/
    def get_column(self, *columns, **kwargs):
        """Retrieve the name of the column associated to a given field (e.g., manually
        defined), depending on the language.
        
                >>> fac.get_column(columns=['col1', 'col2'], ilang=None, olang=None)
        """
        columns = (columns not in ((None,),()) and columns[0])          or \
                    kwargs.pop('columns', None)                                 
        if columns in (None, ()):
            pass # will actually return all columns in that case
        elif isinstance(columns, string_types):     
            columns = (columns,)
        elif not (isinstance(columns, Sequence) and all([isinstance(col, string_types) for col in columns])):   
             raise TypeError("Wrong input format for columns - must be a (list of) string(s)")
        try:
            langs = list(self.columns[0].keys())
        except:
            langs = []
        # langs = list(dict.fromkeys([LANG, *langs])) # reorder with LANG first default... 
        ilang = kwargs.pop('ilang', self.lang) # OLANG
        if ilang is None and not columns in (None, ('',), ()):
            # try to guess the language in the index
            #for ilang in langs:
            #    try:
            #        assert set(columns).difference([col[ilang] for col in self.columns]) == set()
            #    except:    continue
            #    else:      break
            f = lambda text : Interpret.detect(text)
            try:                        assert False and f(-1)
            except TypeError:
                ilang = Interpret.detect((' ').join(columns.values()))
            else:
                ilang = self.lang # None 
        try:
            assert ilang is not None and ilang in LANGS
        except AssertionError:
            raise IOError("Input language '%s' not recognised" % ilang)            
        olang = kwargs.pop('olang', self.config.get('lang')) 
        try:
            assert olang is not None and olang in LANGS
        except AssertionError:
            raise IOError("Output language '%s' not recognised" % olang)            
        try:
            assert ilang in langs or ilang == self.lang
        except AssertionError:
            f = lambda cols: Interpret.translate(cols, ilang=self.lang, olang=ilang, **kwargs)
            try:                    f(-1)#analysis:ignore
            except TypeError:
                tcols = f([col[self.lang] for col in self.columns])
                [col.update({ilang: t}) for (col,t) in zip(self.columns, tcols)]
            except ImportError:     
                pass
        except KeyError:
             pass # raise IOError('no columns available')
        try:
            assert (olang in langs and 'filt' not in kwargs) or olang == self.lang
            # if you add a filter, translation is forced
        except AssertionError:
            f = lambda cols: Interpret.translate(cols, ilang=self.lang, olang=olang, **kwargs)
            try:                    f(-1)#analysis:ignore
            except TypeError:
                tcols = f([col[self.lang] for col in self.columns])
                [col.update({olang: t}) for (col,t) in zip(self.columns, tcols)]
            except ImportError:     
                pass
        except KeyError:
             pass # raise IOError('no columns available')
        if columns in (None, ('',), ()): # return all translations
            return [col[olang] for col in self.columns]
        ncolumns = {}
        [ncolumns.update({col[ilang]: col}) for col in self.columns]
        #[ncolumns.update({col[ilang]: col.pop(ilang) and col})    \
        #                 for col in [col.copy() for col in self.columns]]
        res = [ncolumns[col].get(olang) or ncolumns[col].get(ilang)   \
               if col in ncolumns.keys() else None for col in columns]
        return res if len(res)>1 else res[0]

    #/************************************************************************/
    def set_column(self, *columns, **kwargs):
        """Rename (and cast) the column associated to a given field (e.g., as identified
        in the index), depending on the language.
        
                >>> fac.set_column(columns={'newcol': 'oldcol'})
        """
        columns = (columns not in ((None,),()) and columns[0])        or \
                    kwargs.pop('columns', None)                     
        if columns in (None, ()):
            columns = {}  # will actually set all columns in that case
        elif not isinstance(columns, Mapping):
            raise TypeError("Wrong input format for columns - must be a mapping dictionary")
        force_rename = kwargs.pop('force', False)
        lang = kwargs.pop('lang', self.lang)
        idate = kwargs.pop('date', self.date)
        # dumb renaming from one language to the other
        if columns=={} and lang!=self.lang:
            try:
                self.data.rename(columns={col[self.lang]: col[lang] for col in self.columns}, inplace=True)
            except:     pass
            # self.lang = lang
        if lang != self.lang:
            # columns = {k: self.get_column(v, ilang=lang, olang=self.lang) for (k,v) in columns.items() if v not in (None,'')}
            columns = {k:col[self.lang] for col in self.columns      \
                       for (k,v) in columns.items() if (col[lang]==v and v not in ('',None))}
        fields = {}
        try:
            INDEX = self.config['index']
        except:
            # INDEX = {}
            return
        for (ind, field) in columns.items():
            if field is None:
                if force_rename is False:       
                    # warnings.warn("\n! column '%s' will not be reported in the formatted output table !" % ind)
                    continue
                else:                           
                    field = ind 
            ofield = INDEX[ind]['name'] if ind in INDEX.keys() and force_rename is False else ind
            cast = BASETYPE[INDEX[ind]['type']]            
            #t if ind in self.index: # update the index: this will inform us about which renamings were successful
            #t    self.index.update({ind: ofield})              
            if field == ofield:
                fields.update({field: ofield}) # add it the first time it appears
                continue
            elif field in fields: # dumb copy
                self.data[ofield] = self.data[fields[field]]
                continue
            else:
                fields.update({field: ofield}) # deal with duplicated columns
                self.data.rename(columns={field: ofield}, inplace=True)          
            if cast == self.data[ofield].dtype:
                continue
            elif cast == datetime:                
                self.data[ofield] = Dataframe.out_date(self.data, ofield, self.config.get('date') or '', ifmt=idate) 
            else:
                self.data[ofield] = Dataframe.out_cast(self.data, ofield, cast)
        return columns 

    #/************************************************************************/
    def clean_column(self, *columns, **kwargs):
        """Filter the dataframe.
        """
        columns = (columns not in ((None,),()) and columns[0])        or \
                    kwargs.pop('drop', [])
        if isinstance(columns, string_types):
            columns = [columns,]
        elif not(columns in (None, ())                                  or \
                 (isinstance(columns, Sequence) and all([isinstance(col,string_types) for col in columns]))):
            raise TypeError("Wrong input format for drop columns - must be a (list of) string(s)")
        keepcols = kwargs.pop('keep', [])                     
        if isinstance(keepcols, string_types):
            keepcols = [keepcols,]
        elif not(isinstance(keepcols, Sequence) and all([isinstance(col,string_types) for col in keepcols])):
            raise TypeError("Wrong input format for keep columns - must be a (list of) string(s)")
        force_keep = kwargs.pop('force', False)                     
        # lang = kwargs.pop('lang', None) # OLANG
        try:
            INDEX = self.config['index']
        except:
            # INDEX = {}
            return
        for i, col in enumerate(columns):
            try:        assert col in self.data.columns
            except:
                try:        assert col in self.index
                except:     continue
                else:
                    # col = [col_[self.lang] for col_ in self.columns if col_[lang]==col][0]
                    columns.pop(i)
                    #t columns.insert(i, self.index[col])
                    columns.insert(i, INDEX[col]) #t
            else:       continue
        for i, ind in enumerate(keepcols):
            try:        assert ind in self.data.columns
            except:
                try:        assert ind in self.index and self.index[ind] is not None
                except:     continue
                else:
                    keepcols.pop(i)
                    #t keepcols.insert(i, self.index[ind])
                    keepcols.insert(i, INDEX[col])
            else:       continue
        # refine the set of columns to actually drop
        columns = list(set(columns).difference(set(keepcols)))
        # drop the columns
        #try:
        #    self.data.drop(columns=columns, axis=1, inplace=True)
        #except:     pass
        for col in columns:
            try: # we make a try per column...
                self.data.drop(columns=col, axis=1, inplace=True)
            except:     pass
        # say it in a more Pythonic way:
        # [self.data.drop(col, axis=1) for col in columns if col in self.data.columns]
        if force_keep is False:
            return
        # 'keep' the others, i.e. when they dont exist create with NaN                
        for ind in keepcols:
            if ind in self.data.columns:
                continue
            cast = BASETYPE[INDEX[ind]['type']] if ind in INDEX.keys() else object    
            if cast == datetime:    cast = str
            try:
                self.data[ind] = pd.Series(dtype=cast)
            except:     pass
    
    #/************************************************************************/
    def define_place(self, *place, **kwargs):
        """Build the place field as a concatenation of existing columns.
        
                >>> fac.define_place(place=['street', 'no', 'city', 'zip', 'country'])
        """
        lang = kwargs.pop('lang', self.config.get('lang'))  
        place = (place not in ((None,),()) and place[0])            or \
                kwargs.pop('place', None)                           or \
                self.place
        try:
            assert place in ([],None,'')
            tplace = Interpret.translate(GeoService.PLACE, ilang='en', olang=lang)
        except (AssertionError,IOError,OSError):
            pass
        else:
            place = tplace
        #force_match = kwargs.pop('force_match', False)
        if isinstance(place, Mapping):
            kwargs.update(place)
            place = list(place.keys())
        self.place = place if isinstance(place, string_types) else 'place' 
        if isinstance(place, string_types):
            place = [place,] # just to be sure...
        try:
            INDEX = self.config['index']
        except:
            INDEX = {}
        if 'place' in INDEX.keys() and not 'place' in self.index: # actually not recorded: always False
            self.index.update({'place': 'place'}) # it is created on the fly
        try:
            assert self.place in self.data.columns
        except:
            pass
        else:
            return
        for i in range(len(place)):
            field = place.pop(i)
            if field in kwargs:
                self.set_column(columns={field: kwargs.pop(field), 'lang': lang})
            if field in self.index:
                ofield = self.index[field] or field
            else:         
                ofield = field
            place.insert(i, ofield)
        if not set(place).issubset(self.data.columns):
            place = list(set(place).intersection(self.data.columns))
        self.data[self.place] = self.data[place].astype(str).apply(', '.join, axis=1)
                
    #/************************************************************************/
    def find_location(self, *latlon, **kwargs):
        """Retrieve the geographical coordinates, may that be from existing lat/lon
        columns in the source file, or by geocoding the location name. 
        
            >>> fac.find_location(latlon=['lat', 'lon'])
        """
        latlon = (latlon not in ((None,),()) and latlon)            or \
                kwargs.pop('latlon', None)                        
        if not isinstance(latlon, string_types) and isinstance(latlon, Sequence):
            if isinstance(latlon, Sequence) and len(latlon) == 1:
                latlon = latlon[0]
        if isinstance(latlon, string_types):
            lat = lon = latlon
        elif isinstance(latlon, Sequence):
            lat, lon = latlon
        elif not latlon in ([],None):
            raise TypeError("Wrong lat/lon fields - must be a single or a pair of string(s)")
        order = kwargs.pop('order', 'lL')
        place = kwargs.pop('place', self.place)
        # lang = kwargs.pop('lang', self.lang)
        if latlon in ([],None):
            lat, lon = self.index.get('lat', 'lat'), self.index.get('lon', 'lon')
            order = 'lL'
        try:
            INDEX = self.config['index']
        except:
            olat, olon = 'lat', 'lon'
            otlat, otlon = None, None
        else:
            olat, olon = INDEX['lat']['name'], INDEX['lon']['name']
            otlat, otlon = INDEX['lat']['type'], INDEX['lon']['type']
        if lat == lon and lat in self.data.columns: #self.columns[lang]
            latlon = lat
            if order == 'lL':           lat, lon = olat, olon
            elif order == 'Ll':         lat, lon = olon, olat
            else:
                raise IOError("Unknown order keyword - must be 'lL' or 'Ll'")
            self.data[[lat, lon]] = self.data[latlon].str.split(pat=r'\s+', n=1, expand=True) #.astype(float)
            geo_qual = 1
        elif lat in self.data.columns and lon in self.data.columns: 
        # elif lat in self.columns[lang] and lon in self.columns[lang]: 
            if lat != olat:
                self.data.rename(columns={lat: olat}, inplace=True)
            if lon != olon:                
                self.data.rename(columns={lon: olon}, inplace=True)
            geo_qual = 1
        else:
            if not(isinstance(place, string_types) and place in self.data.columns):
                self.define_place(**kwargs)
            #elif set(self.place).difference(set(self.data.columns)) == {}: 
            #    self.set_column(**kwargs)         
            f = lambda place : self.geoserv.locate(place)
            try:                        f(coder=-1)
            except TypeError:
                self.data[olat], self.data[olon] = zip(*self.data[self.place].apply(f))                                     
                self.proj = None
            except ImportError:
                raise IOError("No geocoder available")
            geo_qual = None # TBD
        try:    
            ind = INDEX['geo_qual']['name']
        except:
            pass
        else:
            self.data[ind] = geo_qual 
            if not 'geo_qual' in self.index: 
                self.index.update({'geo_qual': ind})
        # no need: self.columns.extend([{'en': ind}])
        if not 'lat' in self.index:
            self.index.update({'lat': olat})
        if not 'lon' in self.index:
            self.index.update({'lon': olon})
            # no need: self.columns.extend([{'en': olat}, {'en': olon}}])
        PROJ = self.config.get('proj')
        if PROJ is not None and self.proj not in (None,'') and self.proj != PROJ:
            f = lambda lat, lon : self.geoserv.project([lat, lon], iproj=self.proj, oproj=PROJ)
            try:                        f('-1')
            except TypeError:
                self.data[olat], self.data[olon] = zip(*self.data[[olat, olon]].apply(f))
            except ImportError:
                raise IOError("No projection transformer available")
        # cast
        # self.data[olat], self.data[olon] = pd.to_numeric(self.data[olat]), pd.to_numeric(self.data[olon])
        try:
            self.data[olat], self.data[olon] =                              \
                self.data[olat].astype(BASETYPE.get(otlat)),    \
                self.data[olon].astype(BASETYPE.get(otlon))
        except:
            pass
        
    #/************************************************************************/
    def prepare_data(self, *args, **kwargs):
        """Abstract method for data preparation.
        
            >>> fac.prepare_data(*args, **kwargs)
        """
        pass
    
    #/************************************************************************/
    def format_data(self, **kwargs):
        """Run the formatting of the input data according to the harmonised template
        as provided by the index metadata.
        
            >>> fac.format_data(**index)
        """
        _columns = kwargs.pop('index', {})
        if isinstance(_columns, string_types):
            _columns = {_columns: None}  
        elif isinstance(_columns, Sequence):
            _columns = dict(zip(_columns,_columns))
        elif not isinstance(_columns, Mapping):
            raise TypeError("Wrong format for input index - must a mapping dictionary")
        lang = kwargs.pop('lang', self.config.get('lang'))
        if not isinstance(lang, string_types):
            raise TypeError("Wrong format for language - must a string")
        try:
            columns = self.index.copy()
            columns.update(_columns) # index overwrites whatever is in oindex
        except:
            raise IOError
        try:
            assert lang == self.lang
        except:            
            columns = {k: self.get_column(v, ilang=lang, olang=self.lang)  or \
                        self.get_column(v, olang=self.lang)                 \
                     for (k,v) in columns.items() if v not in (None,'')}
        try:
            assert columns != {}
        except: # not vey happy with this, but ok... it's a default!
            try:
                columns = {col[lang]: col[self.lang] for col in self.columns}
            except:
                raise IOError("Nothing to match to the input columns - check the (empty) index")
        # check for country- and redate-related columns - special cases
        for attr in ['country', 'cc', 'refdate']:
            if attr in columns:
                _column = columns[attr] or attr
                if not _column in self.data.columns:   
                    self.data[_column] = getattr(self, '__' + attr, None) 
                if not attr in self.index:   
                    self.index.update({attr: _column})
            else:       pass
        # find the locations associated to the data
        latlon = [columns.get(l, l) for l in ['lat', 'lon']]
        ## define the place: we actually skip this (see 'assert False' below), and 
        ## do it only if needed when running find_location later
        #try:
        #    assert False # 
        #    place = [key for key in columns.keys() if key in PLACE]
        #    self.define_place(place = place)
        #except:
        #    pass
        try:
            latlon = [columns.get(l, l) for l in ['lat', 'lon']]
            self.find_location(latlon = latlon)
        except:
            warnings.warn("\n! Location not assigned for data !")            
        finally:
            [columns.pop(l,None) for l in ['lat', 'lon']]
        ## update oindex with index (which has been modified by get_column and
        ## does not contain ['lat','lon'])
        # self.index.update(index)
        # reset the columns with the right exptected names 
        try:
            self.set_column(columns = columns)
        except:     pass
        # clean the data so that it matches the template; keep even those fields
        # from index which have no corresponding column
        #t keep = [v if v is not None and k in INDEX else INDEX[k]['name'] for (k,v) in self.index.items()]
        try:
            INDEX = self.config['index']
        except:
            keepcol = columns # self.index.keys()
        else:
            keepcol = [INDEX[k]['name'] for (k,v) in self.index.items()    \
                       if k in INDEX and v is not None]
        try:
            self.clean_column(list(self.data.columns), keep = keepcol)
        except:
            pass
        if _INDEX_ALWAYS_AS_ILANG is True:
            self.index.update(columns)
        
    #/************************************************************************/
    def dumps_data(self, **kwargs):
        """Return JSON or GEOJSON formatted data.
        
            >>> dic = fac.dumps_data(fmt='json')
            >>> geom = fac.dumps_data(fmt='geojson')
        """
        fmt = kwargs.pop('fmt', None)
        if fmt is None: # we give it a default value...
            fmt = 'json'
        elif not isinstance(fmt, string_types):
            raise TypeError("Wrong input format - must be a string key")
        else:
            fmt = fmt.lower()
        FMT = self.config.get('fmt') or {}
        if not fmt in FMT:
            raise IOError("Wrong input format - must be any string among '%s'" % list(FMT.keys()))
        elif fmt in ('csv','gpkg'):
            raise IOError("format '%s' not supported" % fmt)
        INDEX = self.config.get('index') or {}
        columns = kwargs.pop('columns', None) or [ind['name'] for ind in INDEX.values()]
        latlon = kwargs.pop('latlon', None) or [INDEX['lat']['name'], INDEX['lon']['name']]
        if fmt == 'geojson':
            try:
                results = Dataframe.to_geojson(self.data, columns = columns, latlon = latlon)
            except:
                raise IOError("Issue when creating GEOJSON geometries")
        elif  fmt == 'json':
            try:
                results = Dataframe.to_json(self.data, columns = columns)
            except:
                raise IOError("Issue when creating JSON attributes")
        try:
            assert kwargs.pop('as_str', False) is False
        except:
            kwargs.update({'ensure_ascii': kwargs.pop('ensure_ascii', False)})
            try:
                return Json.dumps(results, **kwargs)
            except:     
                raise IOError("issue when dumping '%s' attributes" % fmt.upper())
        else:
            return results
            
    #/************************************************************************/
    def dump_data(self, *dest, **kwargs):
        """Store transformed data in GEOJSON or CSV formats.
        
            >>> fac.dump_data(dest=filename, fmt='csv')
            >>> fac.dump_data(dest=filename, fmt='geojson')
        """
        dest = (dest not in ((None,),()) and dest[0])               or \
             kwargs.pop('dest', None)                               or \
             self.dest        
        fmt = kwargs.pop('fmt', None)
        if fmt is None: # we give it a default value...
            fmt = 'csv'
        elif not isinstance(fmt, string_types):
            raise TypeError("Wrong input format - must be a string key")
        else:
            fmt = fmt.lower()
        encoding = kwargs.pop('enc', self.config.get('enc'))
        sep = kwargs.pop('sep', self.config.get('sep'))
        date = kwargs.pop('date', self.config.get('date'))#analysis:ignore
        FMT = self.config.get('fmt') or {}
        if not fmt in FMT:
            raise TypeError("Wrong input format - must be any string among '%s'" % list(FMT.keys()))
        if dest in (None,''):
            dest = osp.abspath(osp.join(self.config.get('path'), fmt, self.config.get('file') % (self.cc, FMT.get(fmt))))
            warnings.warn("\n! Output data file '%s' will be created" % dest)
        columns, latlon = kwargs.pop('columns', None), kwargs.pop('latlon', None)
        INDEX = self.config.get('index') or {}
        if columns is None or latlon is None: 
            try:
                columns = [ind['name'] for ind in INDEX.values()]
                #columns = list(set(self.data.columns).intersection(set([ind['name'] for ind in INDEX.values()])))
                assert columns not in (None,[])
            except:
                raise IOError('Geographic lat/lon columns not set')
        if latlon in (None,[]): 
            try:
                olat, olon = INDEX['lat']['name'], INDEX['lon']['name'] 
                assert olat in columns and olon in columns
            except:
                raise IOError('Geographic lat/lon columns not set')
        self.data.reindex(columns = columns)
        if fmt == 'csv':
            kwargs.update({'header': True, 'index': False, 
                           'encoding': encoding, 'sep': sep})
            try:
                self.data.to_csv(dest, columns = columns, **kwargs) # date_format=date
            except:
                raise IOError("Issue when creating CSV file")
        elif fmt in ('json','geojson'):
            kwargs.update({'fmt': fmt, 'as_str':False, 'latlon': latlon})
            try:
                results = self.dumps_data(columns = columns, **kwargs)
            except:
                raise IOError("Issue when creating %s geometries" % fmt.upper())
            with open(dest, 'w', encoding=encoding) as f:
                kwargs.update({'ensure_ascii': kwargs.pop('ensure_ascii', False)})
                try:
                    Json.dump(results, f, **kwargs)
                except:
                    raise IOError("Impossible saving metadata file")
        elif fmt == 'gpkg':
            results = Dataframe.to_gpkg(self.data, columns = columns)#analysis:ignore
        return
    
    #/************************************************************************/
    def dumps_config(self, **kwargs):
        warnings.warn("\n! Method not implemented !")
        return
    
    #/************************************************************************/
    def dump_config(self, *dest, **kwargs):
        self.config.dump()
        return
    
    #/************************************************************************/
    def update_meta(self):    
        """Update the metadata file.
        """
        meta = deepcopy(self.meta.to_dict()) # self.meta.__dict__
        for attr in meta.keys():
            if attr == 'index':
                # NO: meta.update({'index': self.index})
                pass
            elif attr == 'country':
                meta.update({'country': isoCountry(self.cc)})
            elif attr == 'lang':
                meta.update({'lang': isoLang(self.lang)})
            else:
                try:
                    meta.update({attr: getattr(self,attr)})
                except:         pass
        self.meta.update(meta)
    
    #/************************************************************************/
    def dumps_meta(self, **kwargs):
        """Dump metadata as output JSON dictionary.
        
            >>> meta = fac.dumps_meta()
        """# basically... nothing much more than self.meta.to_dict()
        if _META_ALWAYS_UPDATED is False:
            self.update_meta()
        try:
            assert kwargs.pop('as_str', False) is False
            return self.meta.to_dict()
        except AssertionError:
            kwargs.update({'ensure_ascii': kwargs.pop('ensure_ascii', False)})
            try:
                return Json.dumps(self.meta.to_dict(), **kwargs)
            except:
                raise IOError("Impossible dumping metadata file")

    #/************************************************************************/
    def dump_meta(self, *dest, **kwargs):
        """Dump metadata into a JSON file.
        
            >>> fac.dump_meta(dest=metaname)
        """
        dest = (dest not in ((None,),()) and dest[0])               or \
             kwargs.pop('dest', None)   
        fmt = kwargs.pop('fmt', None)
        if fmt is None: 
            fmt = 'json'
        elif not isinstance(fmt, string_types):
            raise TypeError("Wrong input format - must be a string key")
        else:
            fmt = fmt.lower()
        if fmt != 'json':
            raise IOError("Metadata output to a JSON format only")
        if dest is None:   
            try:
                dest = osp.join(PACKPATH, self.type, '%s%s.json' % (self.cc, self.type))
            except:
                dest = osp.join(PACKPATH, '%s.json' % self.cc)
            warnings.warn("\n! Metadata file '%s' will be created" % dest)
        if _META_ALWAYS_UPDATED is False:
            self.update_meta()
        # self.meta.dump(dest)
        with open(dest, 'w', encoding=self.enc) as f:
            kwargs.update({'ensure_ascii': kwargs.pop('ensure_ascii', False)})
            try:
                Json.dump(self.meta.to_dict(), f, **kwargs)
            except:
                raise IOError("Impossible saving metadata file")
          

#%% 
#==============================================================================
# Function datnatFactory
#==============================================================================
def datnatFactory(*args, **kwargs):
    """Generic function to derive a class from the base class :class:`BaseFacility`
    depending on specific metadata and a given geocoder.
    
        >>>  NewNatDat = datnatFactory(config=None, meta=None, country=None, coder=None)
        
    Examples
    --------
    
        >>>  NewHCS = datnatFactory(HCS, country=CC1, coder={'Bing', yourkey})
        >>>  NewFacility = datnatFactory(country=CC2, coder='GISCO')
    """
    basecls = BaseDatNat # kwargs.pop('base', BaseDatNat)
    attributes = {}    
    # check facility to define output data configuration format
    if args in ((),(None,)):        cfg = None
    else:                           cfg = args[0]
    cfg = cfg or kwargs.pop('config', None)
    try:
        assert cfg is None or isinstance(cfg, (Mapping,MetaDat))  
    except AssertionError:
        raise TypeError("Configuration type '%s' not recognised - must be a dictionary or %s" % (type(cfg),MetaDat.__name__))
    if cfg is None:
        config = None
    #elif isinstance(cfg, string_types):
    #    try:
    #        config = CONFIGINFO.get(cfg) 
    #    except AttributeError:
    #        raise TypeError("config string '%s' not recognised ")
    #    except NameError: # there is no such a thing like a global OCFGINFO variable
    #        raise IOError("config type '%s' not recognised")              
    #    try:
    #        config = MetaDat(deepcopy(config))
    #    except: 
    #        raise IOError("config type '%s' not recognised")              
    elif isinstance(cfg,MetaDat):
        config = cfg.copy()
        try:
            config = cfg.copy()
        except:
            raise IOError("Configuration metadata '%s' not recognised" % str(cfg))
    elif isinstance(cfg, Mapping):
        config = MetaDat(cfg) 
        try:
            config = MetaDat(cfg)  
        except:
            raise IOError("Configuration dictionary '%s' not recognised" % cfg)
    try:
        CATEGORY = config.category if hasattr(config, 'category') else config.get("category")
        assert CATEGORY is None or isinstance(CATEGORY,(Mapping,string_types))
    except:
        CATEGORY = None
    else:
        attributes.update({'CATEGORY': CATEGORY})
    # check metadata of input data
    metadata = kwargs.pop('meta', None)
    try:
        assert metadata is None or isinstance(metadata,(string_types,Mapping,MetaDatNat))
    except AssertionError:
        raise TypeError("Metadata type '%s' not recognised - must be a filename, dictionary or %s" % (type(metadata),MetaDatNat.__name__))
    if metadata is None:
        meta = None # meta = MetaDatNat({})
    elif isinstance(metadata,MetaDatNat):
        try:
            meta = metadata.copy()
        except:
            raise IOError("Metadata '%s' not recognised "  % metadata)
    elif isinstance(metadata, (string_types, Mapping)):
        try:
            meta = MetaDatNat(metadata)
        except:
            raise IOError("Metadata '%s' not recognised " % metadata)
    # check country
    try:
        COUNTRY = meta.get('country') if meta is not None and 'country' in meta else kwargs.pop('country', None)
        assert COUNTRY is None or isinstance(COUNTRY,(Mapping,string_types))
    except AssertionError:
        raise IOError("Country type '%s' not recognised - must be a string or a dictionary" % type(COUNTRY))
    try:
        COUNTRY = isoCountry(COUNTRY)
    except:
        CC = ''
    else:
        CC = COUNTRY.get('code')
        attributes.update({'COUNTRY': {CC: COUNTRY.get('name')}})
    ## check language
    #lang = kwargs.pop('lang', None)
    #if lang not in (None,'',{):
    #    lang = TextProcess.isoLang(lang)
    #    LANG = lang.get('code')
    #    attributes.update({'LANG': LANG})
    #else:
    #   LANG = ''
    # check survey year
    try:
        YEAR = meta.get('year') if meta is not None and 'year' in meta else kwargs.pop('year', None)
        assert YEAR is None or isinstance(YEAR,int)
    except AssertionError:
        raise IOError("Year type '%s' not recognised - must be an integer" % type(YEAR))
    else:
        attributes.update({'YEAR': YEAR})
    # check geocoder
    CODER = kwargs.pop('coder', None)
    try:
        assert CODER is None or isinstance(CODER,(string_types,Mapping))
    except AssertionError:
        raise TypeError("Coder type '%s' not recognised - must be a string or a dictionary" % type(CODER))
    #else:
    #    attributes.update({'CODER': CODER})
    if not CODER in ({}, ''): # None accepted as default geocoder!
        try:
            geoserv = GeoService(CODER)
        except:
            raise IOError("Geocoder '%s' not recognised " % CODER)
    else: 
        geoserv = None
    # redefine the initialisation method
    def __init__(self, *args, **kwargs):
        # one configuration dictionary defined 'per facility'
        try:
            self.config = None if config is None else config.copy() 
            # note: creating a "copy" actually creates another TypeFacility instance, 
            # so that this attribute differs from one instance to the other!
        except:
            pass
        # one metadata dictionary defined 'per country'
        try:
            self.meta = None if meta is None else meta.copy()
            # ibid: creating a "copy" actually creates another MetaFacility instance
        except:
            pass
        # the geocoder is defined 'per country', i.e. you may use different geocoders 
        # for different countries since the quality (e.g., OSM) may differ
        try:
            self.geoserv = geoserv
        except:
            pass
        #for key, value in kwargs.items():
        #    # here, the argnames variable is the one passed to the
        #    # ClassFactory call
        #    if key not in argnames:
        #        raise TypeError("Argument %s not valid for %s" 
        #            % (key, self.__class__.__name__))
        #    setattr(self, key, value)
        basecls.__init__(self, *args, **kwargs)
        # super(self.__class__, self).__init__(*args, **kwargs)) ... abstract, we don't know the class yet
    attributes.update({"__init__": __init__})
    try:
        name = '%s%s' % (CC.upper(), CATEGORY.lower())
    except:
        name = 'New%s' % basecls.__name__.replace('Base','')
    return type(name, (basecls,), attributes)
 
