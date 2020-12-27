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

*call*:         :mod:`pyeudatnat`, :mod:`pyeudatnat.meta`, :mod:`pyeudatnat.io`,
                :mod:`pyeudatnat.text`, :mod:`pyeudatnat.geo`

**Contents**
"""

# *credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_
# *since*:        Tue Fri 20 23:14:24 2020

#%% Settings

import io, sys, re
from os import path as osp
import pprint, logging

from collections.abc import Mapping, Sequence
from six import string_types

from datetime import datetime, timedelta
from copy import deepcopy

import numpy as np
import pandas as pd

from pyeudatnat import PACKPATH, COUNTRIES, AREAS
from pyeudatnat.meta import MetaDat, MetaDatNat
from pyeudatnat.misc import Object, Structure, Type, FileSys
from pyeudatnat.misc import DEF_DATETIMEFMT
from pyeudatnat.io import Json, Frame, Buffer
from pyeudatnat.io import FORMATS, DEF_FORMATS, DEF_FORMAT, ENCODINGS, DEF_ENCODING, DEF_SEP
from pyeudatnat.text import Interpret, isoLang
from pyeudatnat.text import LANGS, DEF_LANG
from pyeudatnat.geo import isoCountry, Service as GeoService
from pyeudatnat.geo import DEF_CODER, DEF_PLACE


_IS_META_UPDATED    = False

PROCESSES           = [ 'fetch', 'load', 'prepare', 'clean', 'translate',
                        'locate', 'format', 'save' ]


#%% Core functions/classes

#==============================================================================
# Class BaseDatNat
#==============================================================================

class BaseDatNat(object):
    """Base class used to represent national data sources.

        >>> dat = BaseDatNat(**metadata)
    """

    CATEGORY = None
    COUNTRY = None # class attribute... that should not be different from cc
    VERSION = None # for future...

    #/************************************************************************/
    def __init__(self, *args, **kwargs):
        # self.__config, self.__metadata = {}, {}
        # self.__options = {}
        self.__data, self.__buffer = None, None # data, content
        self.__columns, self.__index = {}, []
        self.__language = None
        self.__refdate = None
        self.__geocoder, self.__geoservice = None, None
        self.__proj = None
        try:
            # meta should be initialised in the derived class
            assert self.__metadata not in ({},None)
        except(AttributeError,AssertionError):
            if not args in ((),(None,)):
                self.meta = MetaDat(args[0])
            else:
                #logging.warning("\n! No metadata parsed !")
                self.meta = MetaDat()
        try:
            # config should be initialised in the derived class
            assert self.__config not in ({},None)
        except (AttributeError,AssertionError):
            self.config = MetaDat()
        try:
            assert self.__options not in ({},None)
        except (AttributeError,AssertionError):
            self.options = {p:{} for p in PROCESSES} # dict.fromkeys(PROCESSES, {})
        try:
            self.__geocoder = kwargs.pop('coder', self.geocoder)
        except:
            pass
        # retrieve type
        self.category = kwargs.pop('category', self.config.get('category') or self.CATEGORY)
        # retrieve country name and code
        country = isoCountry(self.meta.get('country') or self.COUNTRY)
        self.cc = kwargs.pop('cc', None if country in ({},None) else country.get('code'))
        # retrieve languge of the input data
        lang = isoLang(self.meta.get('lang'))
        self.lang = kwargs.pop('lang', None if lang in ({},None) else lang.get('code'))
        path, file = kwargs.pop('path', self.meta.get('path') or ''), kwargs.pop('file',self.meta.get('file') or '')
        # path = osp.abspath(path)
        self.file = None if file in (None,'') and path in (None,'')         \
            else (file if path in (None,'')                                 \
            else (osp.join(path, file) if isinstance(file, string_types)    \
            else ([osp.join(path, f) for f in file] if isinstance(file, Sequence) \
            else path)))
        self.src = kwargs.pop('src', self.meta.get('src') or None) # to avoid ''
        # retrieve input options / parameters for the various processing operations
        self.options.update(self.meta.get('options',{}))
        self.options = kwargs.pop('options', self.options)
        # retrieve caching arguments
        self.cache = {'caching':kwargs.pop('caching',False),
                      'cache_store':None, 'cache_expire':0, 'cache_force':True}
        if self.cache['caching'] is True:
            self.cache.update({'cache_store': kwargs.pop('cache_store', ''),
                               'cache_expire': kwargs.pop('cache_expire', 0),
                               'cache_force': kwargs.pop('cache_force',False)
                              })
        # retrieve reference date, if any
        self.date = kwargs.pop('date', None)
        # retrieve the input data projection
        self.proj = kwargs.pop('proj', self.meta.get('proj')) # projection system
        # retrieve columns when already known
        cols = kwargs.pop('cols', None)
        self.cols = cols or self.meta.get('columns') or []    # header columns
        [col.update({self.lang: col.get(self.lang) or ''}) for col in self.cols] # ensure there are 'locale' column names
        # retrieve matching columns when known
        index = kwargs.pop('index', None)   # index
        self.idx = index or self.meta.get('index') or {}

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
        elif attr.startswith('opts_'):
            try:        return self.options.get(attr[len('opts_'):])
            except:     pass
        else:
            pass
        try:        return object.__getattribute__(self, attr)
        except AttributeError:
            raise AttributeError("%s object has no attribute '%s'" % (type(self),attr))
            # ignore what's next...
            try:
                return object.__getattribute__(self, '__' + attr)
            except (AttributeError,AssertionError):
                try:
                    return getattr(self.__class__, attr)
                except (AttributeError,AssertionError):
                    raise AttributeError("%s object has no attribute '%s'" % (type(self),attr))

    #/************************************************************************/
    @property
    def meta(self):
        return self.__metadata # or {}
    @meta.setter
    def meta(self, meta):
        if not (meta is None or isinstance(meta, (MetaDatNat,Mapping))):
            raise TypeError("Wrong format for country METAdata: '%s' - must be a dictionary" % type(meta))
        self.__metadata = meta

    @property
    def config(self):
        return self.__config # or {}
    @config.setter
    def config(self, cfg):
        if not (cfg is None or isinstance(cfg, (MetaDat,Mapping))):
            raise TypeError("Wrong format for CONFIGuration info: '%s' - must be a dictionary" % type(cfg))
        self.__config = cfg

    @property
    def options(self):
        return self.__options
    @options.setter#analysis:ignore
    def options(self, opts):
        if opts in (None, {}):
            opts = {p:{} for p in PROCESSES}
        elif not isinstance(opts, Mapping):
            raise TypeError("Wrong format for OPTIONS '%s' - must be a dictionary" % opts)
        elif set(list(opts.keys())).difference(PROCESSES) != set():
            raise IOError("Wrong keys for OPTIONS '%s' - must be any from the list '%s'" % (opts,PROCESSES))
        elif not all(v is None or isinstance(v,Mapping) for v in opts.values()):
            raise TypeError("Wrong format for OPTIONS '%s' values - must be dictionaries" % opts)
        if _IS_META_UPDATED is True and opts not in ({},None):
            self.__metadata.update({'options': opts})
        self.__options = opts

    @property
    def data(self):
        return self.__data # or {}
    @data.setter
    def data(self, data):
        if not (data is None or isinstance(data, (Mapping,Sequence, np.ndarray, #pd.arrays.PandasArray,
                                                  pd.Index,pd. Series, pd.DataFrame))):
            raise TypeError("Wrong format for DATA: '%s' - must be an array, sequence or dictionary" % type(data))
        self.__data = data

    @property
    def buff(self):
        return self.__buffer # or {}
    @buff.setter
    def buff(self, buff):
        # if not (buff is None or isinstance(cont, (Mapping, Sequence, string_types, bytes,
        #                                           # urllib3.response.HTTPResponse, requests.models.Response,
        #                                           io.StringIO, io.BytesIO, io.TextIOBase))):
        #     raise TypeError("Wrong format for CONTENT: '%s' - must be an array, sequence or dictionary" % type(buff))
        self.__buffer = buff

    @property
    def category(self):
        # return self.config.get('category') or self.CATEGORY
        return self.__category # or ''
    @category.setter
    def category(self, cat):
        if cat is None or isinstance(cat, (string_types, Mapping)):
            pass
        #elif isinstance(cat, Mapping):
        #    cat = str(list(typ.values())[0])
        else:
            raise TypeError("Wrong format for CATEGORY: '%s' - must be a string (or a dictionary)" % type(cat))
        self.__config.update({'category': cat})
        self.__category = cat
    #@property
    #def category(self):
    #    return [v['name'] for (k,v) in FACILITIES.items() if k==self.cat][0]

    @property
    def cc(self):
        return self.__cc
    @cc.setter
    def cc(self, cc):
        if cc is None:                          pass
        elif not isinstance(cc, string_types):
            raise TypeError("Wrong format for CC country code '%s' - must be a string" % cc)
        elif not cc in COUNTRIES: # COUNTRIES.keys()
            raise IOError("Wrong CC country code '%s' - must be any valid ISO code from the EU area" % cc)
        elif cc != next(iter(self.COUNTRY)):
            logging.warning("\n! Mismatch with class variable 'CC': %s !" %
                          next(iter(self.COUNTRY)))
        if _IS_META_UPDATED is True and cc is not None:
            self.__metadata.update({'country': {'code': cc, 'name': COUNTRIES[cc]}}) # isoCountry
        self.__cc = cc

    @property
    def country(self):
        return COUNTRIES[self.cc]
    # @property
    # def area(self):
    #     return self.country

    @property
    def geocoder(self):
        return self.__geocoder
    @geocoder.setter
    def geocoder(self, coder):
        if coder is None: pass # avoid raising an error
        else:
            self.__geocoder = GeoService.select_coder(coder)
        self.__geoservice = None

    @property
    def geoserv(self):
        if self.__geoservice is None:
            if self.__geocoder is None:
                raise IOError("GEOCODER needs to be defined before GEOSERVice can be set")
            self.__geoservice = GeoService(self.__geocoder)
        return self.__geoservice

    @property
    def lang(self):
        return self.__language
    @lang.setter
    def lang(self, lang):
        if lang is None:                          pass
        elif not isinstance(lang, string_types):
            raise TypeError("Wrong format for LANGuage type '%s' - must be a string" % lang)
        elif not lang in LANGS: # LANGS.keys()
            raise IOError("Wrong LANGuage '%s' - must be any valid ISO language code" % lang)
        if _IS_META_UPDATED is True and lang not in (None,{}):
            self.__metadata.update({'lang': {'code': lang, 'name': LANGS[lang]}}) # isoLang
        self.__language = lang

    @property
    def date(self):
        return self.__refdate
    @date.setter
    def date(self, refdate):
        if not (refdate is None or isinstance(refdate, (datetime,int))):
            raise TypeError("Wrong format for DATE: '%s' - must be an integer or a datetime" % refdate)
        if _IS_META_UPDATED is True and refdate is not None:
            self.__metadata.update({'date': refdate})
        self.__refdate = refdate

    # @property
    # def fmt(self):
    #     return self.__format
    # @fmt.setter
    # def fmt(self, fmt):
    #     if fmt is None:                          pass
    #     elif not isinstance(fmt, string_types):
    #         raise TypeError("Wrong type for data ForMaT '%s' - must be a string" % fmt)
    #     elif not fmt in Structure.uniq_list(FORMATS):
    #         raise IOError("Wrong ForMaT: '%s' currently not supported" % fmt)
    #     if _IS_META_UPDATED is True and fmt not in (None,''):
    #         self.__metadata.update({'fmt': fmt})
    #     self.__format = fmt

    @property
    def src(self):
        return self.__source
    @src.setter
    def src(self, src):
        if not (src is None or isinstance(src, string_types)):
            raise TypeError("Wrong format for data SOURCE '%s' - must be a string" % src)
        self.__source = src

    @property
    def file(self):
        return self.__file
    @file.setter
    def file(self, file):
        if not (file is None or isinstance(file, string_types) \
                or (isinstance(file, Sequence) and all([isinstance(f, string_types) for f in file]))):
            raise TypeError("Wrong format for source FILE '%s' - must be a (list of) string(s)" % file)
        if _IS_META_UPDATED is True and file is not None:
            self.__metadata.update({'file': None if file is None else osp.basename(file),
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
        elif set(cache.keys()).difference({'caching','cache_store','cache_expire','cache_force'}) != set():
            raise IOError("Keys for CACHE dictionary not recognised")
        if not(cache.get('caching') is None or isinstance(cache['caching'], (str,bool))):
            raise TypeError("Wrong type for CACHING flag")
        elif not(cache.get('cache_store') is None or isinstance(cache['cache_store'], str)):
            raise TypeError("Wrong type for CACHE_STORE parameter")
        elif not(cache.get('cache_expire') is None or     \
                 (isinstance(cache['cache_expire'], (int, timedelta))) and int(cache['cache_expire'])>=-1):
            raise TypeError("Wrong type for CACHE_EXPIRE parameter")
        elif not(cache.get('cache_force') is None or isinstance(cache['cache_force'], bool)):
            raise TypeError("Wrong type for CACHE_FORCE flag")
        self.__cache = cache

    @property
    def proj(self):
        return self.__proj
    @proj.setter#analysis:ignore
    def proj(self, proj):
        if not (proj is None or isinstance(proj, string_types)):
            raise TypeError("Wrong format for PROJection type '%s' - must be a string" % proj)
        if _IS_META_UPDATED is True and proj is not None:
            self.__metadata.update({'proj': proj})
        self.__proj = proj

    @property
    def cols(self):
        return self.__columns  # self.meta.get('cols')
    @cols.setter#analysis:ignore
    def cols(self, cols):
        if cols is None:
            pass # nothing yet
        elif isinstance(cols, string_types):
            cols = [{self.lang: cols}]
        elif isinstance(cols, Mapping):
            cols = [cols,]
        elif isinstance(cols, Sequence) and all([isinstance(col, string_types) for col in cols]):
            cols = [{self.lang: col} for col in cols]
        elif not(isinstance(cols, Sequence) and all([isinstance(col, Mapping) for col in cols])):
            raise TypeError("Wrong Input COLS headers type '%s' - must be a sequence of dictionaries" % cols)
        if _IS_META_UPDATED is True and cols not in (None,[]):
            self.__metadata.update({'columns': cols})
        self.__columns = cols

    @property
    def idx(self):
        return self.__index  # self.meta.get('index')
    @idx.setter#analysis:ignore
    def idx(self, ind):
        if ind is None:
            pass # nothing yet
        elif isinstance(ind, string_types):
            ind = {ind: None}
        elif isinstance(ind, Sequence):
            ind = dict.fromkeys(ind)
        elif not isinstance(ind, Mapping):
            raise TypeError("Wrong Output INDEX type '%s' - must be a dictionary" % ind)
        if _IS_META_UPDATED is True and ind not in ({},None):
            self.__metadata.update({'index': ind})
        self.__index = ind

    #/************************************************************************/
    def get_options(self, name = None, opts = None, process = None):
        try:
            assert name is None or isinstance(name, string_types)
        except:
            raise TypeError("Wrong type for option NAME")
        try:
            assert opts is None or isinstance(opts, Mapping)
        except:
            raise TypeError("Wrong type for option dictionary OPTS")
        else:
            opts = opts or {}
        try:
            assert process is None or isinstance(process, string_types)
        except:
            raise TypeError("Wrong type for PROCESS operation")
        try:
            assert process in PROCESSES
        except:
            raise IOError("Wrong PROCESS operation - must be one among '%s'" % PROCESSES)
        opt_proc = {}
        if process is not None and process in ['save', 'format']:
            opt_proc.update(self.config.get('options', None) or {})
        if process is not None:
            opt_proc.update(self.options.get(process, None) or {})
        if name is None:
            opt_proc.update(opts)
            return opt_proc
        elif name in opts.keys():
            return opts.pop(name)
        elif name in opt_proc.keys():
            return opt_process.get(name)
        else:
            try:
                return getattr(self, name)
            except:
                return None

    #/************************************************************************/
    def fetch_data(self, *src, **kwargs):
        """Load buffer content of source file.

                >>> datnat.fetch_data(*src, **kwargs)
        """
        src = (src not in ((None,),()) and src[0])                          \
            or kwargs.pop('src', None) or self.src
        file = kwargs.pop('file', None) or self.file
        if src in (None,'') and file in (None,''):
             raise IOError("No SRC filename provided - set keyword file attribute/parameter")
        elif not(src is None or isinstance(src, string_types)):
             raise TypeError("Wrong format for SRC data - must be a string")
        elif not(file is None or isinstance(file, string_types)     \
                 or (isinstance(file, Sequence) and all([isinstance(f,string_types) for f in file]))):
             raise TypeError("Wrong format for filename - must be a (list of) string(s)")
        opts_fetch = self.get_options(opts = kwargs, process = 'fetch')
        opts_fetch.update(self.cache)
        self.buff = Buffer.from_file(file, src = src, **opts_fetch)
        if self.src != src:                 self.src = src
        if self.file != file:               self.file = file

    #/************************************************************************/
    def load_data(self, *src, **kwargs):
        """Load data source file.

                >>> datnat.load_data(*src, **kwargs)
        """
        # retrieve a default date format
        ignore_buffer = kwargs.pop('ignore_buffer', False)
        opts_load = self.get_options(opts = kwargs, process = 'load')
        if ignore_buffer is False and self.buff is not None:
            try:
                self.data = Frame.from_buffer(self.buff,  **opts_load)
            except:
                self.data = Frame.from_data(self.buff, **opts_load)
                self.buff = None
            return
        else:
            src = (src not in ((None,),()) and src[0])                          \
                or kwargs.pop('src', None) or self.src
            file = kwargs.pop('file', None) or self.file
            if src in (None,'') and file in (None,''):
                 raise IOError("No SRC filename provided - set keyword file attribute/parameter")
            elif not(src is None or isinstance(src, string_types)):
                 raise TypeError("Wrong format for SRC data - must be a string")
            elif not(file is None or isinstance(file, string_types)     \
                     or (isinstance(file, Sequence) and all([isinstance(f,string_types) for f in file]))):
                 raise TypeError("Wrong format for filename - must be a (list of) string(s)")
            opts_load.update(self.cache)
            self.data = Frame.from_file(file, src = src, **opts_load)
            if self.src != src:             self.src = src
            if self.file != file:           self.file = file
        try:
            assert self.cols not in (None,[],[{}])
        except:
            self.cols = [{self.lang:col} for col in self.data.columns]
        #if set([col[self.lang] for col in self.columns]).difference(set(self.data.columns)) != set():
        #    logging.warning("\n! mismatched data columns and header fields !")
        # if everything worked well, update the fields in case they differ

    #/************************************************************************/
    def get_cols(self, *columns, **kwargs):
        """Retrieve the name of the column associated to a given field (e.g., manually
        defined), depending on the language.

            >>> datnat.get_cols(cols=['col1', 'col2'], ilang=None, olang=None)
        """
        columns = (columns not in ((None,),()) and columns[0])              \
            or kwargs.pop('cols', None)
        if columns in (None, ()):
            pass # will actually return all columns in that case
        elif isinstance(columns, string_types):
            columns = (columns,)
        elif not (isinstance(columns, Sequence) and all([isinstance(col, string_types) for col in columns])):
             raise TypeError("Wrong input format for COLS - must be a (list of) string(s)")
        try:
            langs = list(self.cols[0].keys())
        except:
            langs = []
        # langs = list(dict.fromkeys([LANG, *langs])) # reorder with LANG first default...
        ilang = kwargs.pop('ilang', self.lang)
        force = kwargs.pop('force', False)
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
        olang = kwargs.pop('olang', None)                                   \
            or self.config.get('options', {}).get('lang') or DEF_LANG
        try:
            assert olang is not None and olang in LANGS
        except AssertionError:
            raise IOError("Output language '%s' not recognised" % olang)
        opts_translate = self.get_options(opts = kwargs, process = 'translate')
        try:
            assert ilang in langs or ilang == self.lang
        except AssertionError:
            f = lambda cols:                                                \
                Interpret.translate(cols, ilang = self.lang, olang = ilang, **opts_translate)
            try:                    f(-1)
            except TypeError:
                tcols = f([col[self.lang] for col in self.cols])
                [col.update({ilang: t}) for (col,t) in zip(self.cols, tcols)]
            except ImportError:
                pass
        except KeyError:
            # raise IOError("Language '%s not available - provide with translations" % ilang)
            pass # raise IOError('no columns available')
        try:
            assert (olang in langs and force is not True) or olang == self.lang
            # if you add a filter, translation is forced
        except AssertionError:
            f = lambda cols:                                                \
                Interpret.translate(cols, ilang = self.lang, olang = olang, **opts_translate)
            try:                    f(-1)
            except TypeError:
                tcols = f([col[self.lang] for col in self.cols])
                [col.update({olang: t}) for (col,t) in zip(self.cols, tcols)]
            except ImportError:
                raise IOError("Language '%s not available - provide with translations" % olang)
        except KeyError:
             pass # raise IOError('no columns available')
        if columns in (None, ('',), ()): # return all translations
            return [col[olang] for col in self.cols]
        ncolumns = {}
        [ncolumns.update({col[ilang]: col}) for col in self.cols]
        #[ncolumns.update({col[ilang]: col.pop(ilang) and col})    \
        #                 for col in [col.copy() for col in self.columns]]
        res = [ncolumns[col].get(olang) or ncolumns[col].get(ilang)   \
               if col in ncolumns.keys() else None for col in columns]
        return res if len(res)>1 else res[0]

    #/************************************************************************/
    def _list_cols(self, *columns, **kwargs):
        columns = (columns not in ((None,),()) and columns[0])              \
            or kwargs.pop('cols', None)
        if columns in ((),None):
            columns = self.data.columns
        elif isinstance(columns, string_types):
            columns = [columns,]
        elif not isinstance(columns, (Sequence,Mapping)):
            raise TypeError("Wrong input format for columns - must be a mapping or a list")
        if isinstance(columns, Sequence):
            columns = {c:c for c in columns}
        force_keep = kwargs.pop('force', False)
        lang = kwargs.pop('lang', None) # or DEF_LANG
        nlist = {}
        for ind, col in columns.items():
            if not col in self.data.columns:
                col = self.idx.get(col) or col
            val = [list(c.values()) if lang is None else c[lang]
                   for c in self.cols if col in c.values()]
            if val == []:
                if force_keep is False:
                    continue
                val = [ind]
            val = val[0] if len(val) == 1 else val
            nlist.update({ind: val})
        return nlist

    #/************************************************************************/
    def _match_cols(self, *columns, **kwargs):
        columns = (columns not in ((None,),()) and columns[0])              \
            or kwargs.pop('cols', None)
        if columns in ((),None):
            columns = self.data.columns
        elif isinstance(columns, string_types):
            columns = [columns,]
        elif not isinstance(columns, (Sequence,Mapping)):
            raise TypeError("Wrong input format for columns - must be a mapping or a list")
        if isinstance(columns, Sequence):
            columns =  {c:c for c in columns} # dict(zip(columns, columns))
        force_keep = kwargs.pop('force', False)
        res, inv = {}, {}
        for ind, col in self.idx.items():
            val = [c.values() for c in self.cols if col in c.values()]
            if val == []: continue
            val = list(set(val[0] if len(val) == 1 else val))
            [inv.update({v:inv.get(v,[]) + [ind]}) for v in val if ind not in inv.get(v,[])]
        for ind, col in columns.items():
            # ind = self.config['index'][ind] if ind in self.config['index'].keys() else ind
            col = self.idx[col] if col in self.idx.keys() else col
            if col in self.data.columns:
                res.update({ind: col})
                continue
            val = [c.values() for c in self.cols if col in c.values()]
            if val == []:
                if force_keep is False:  continue # column/field unknown
                val =[[]]
            val = list(set([v for v in val[0] if v in self.data.columns]))
            if val == []:
                if force_keep is False:  continue
                val = [ind]
            res.update({ind: val[0] if len(val) == 1 else val})
        items = list(res.items())
        for ind, col in items:
            col_inv = inv.get(col,[])
            if col_inv is not None:
                [res.update({c: col}) for c in col_inv]
            if force_keep is False and ind not in col_inv:
                res.pop(ind)
        #oindex = self.config.get('index', {})
        #res = {oindex[k].get('name', k):v if k in oindex.keys() else k
        #            for (k,v) in res.items()}
        return res

    #/************************************************************************/
    def _clean_cols(self, *columns, **kwargs):
        """Filter the dataframe.

        >>> datnat._clean_cols(columns, **kwargs)

        """
        columns = (columns not in ((None,),()) and columns[0])              \
                    or kwargs.pop('drop', [])
        if isinstance(columns, string_types):
            columns = [columns,]
        elif not(columns in (None, ())      \
                 or (isinstance(columns, Sequence) and all([isinstance(col,string_types) for col in columns]))):
            raise TypeError("Wrong input format for drop columns - must be a (list of) string(s)")
        keepcols = kwargs.pop('keep', [])
        if isinstance(keepcols, string_types):
            keepcols = [keepcols,]
        elif not(isinstance(keepcols, Sequence) and all([isinstance(col,string_types) for col in keepcols])):
            raise TypeError("Wrong input format for keep columns - must be a (list of) string(s)")
        # lang = kwargs.pop('lang', None) # OLANG
        # refine the set of columns to actually drop
        columns = (set(self._list_cols(columns))
                        .difference(
                            set(self._list_cols(keepcols))
                            )
                        )
        # drop the columns
        self.data.drop(columns = list(columns), axis = 1,
                       inplace = True, errors = 'ignore')

    #/************************************************************************/
    def clean_data(self, *args, **kwargs):
        """Abstract method for data cleansing.

            >>> datnat.clean_data(*args, **kwargs)
        """
        pass

    #/************************************************************************/
    def prepare_data(self, *args, **kwargs):
        """Abstract method for data preparation.

            >>> datnat.prepare_data(*args, **kwargs)
        """
        pass

    #/************************************************************************/
    def get_place(self, *columns, **kwargs):
        """Retrieve the name of the column in the original dataset associated to
        the fields used to build a place/location, depending on the language:

            >>> place = datnat.get_place(cols = ['street', 'no', 'city', 'zip', 'country'],
                                         lang = None)
        """
        columns = (columns not in ((None,),()) and columns[0])              \
            or kwargs.pop('cols', self.meta.get('place')) or DEF_PLACE
        lang = kwargs.pop('lang', None) # or DEF_LANG
        try:
            assert isinstance(columns, (Mapping,Sequence))
        except AssertionError:
            raise TypeError("Wrong type of place COLUMNS name(s) '%s'" % columns)
        if isinstance(columns, Mapping):
            columns = list(columns.keys())
        elif isinstance(columns, string_types):
            columns = [columns,]
        if lang is None:
            columns = self._match_cols(columns, force = True)
        else:
            columns = self._list_cols(columns, lang = lang, force = True)
        columns = columns.values()
        # return list(set(columns).intersection(self.data.columns))
        if not set(columns).issubset(self.data.columns):
            columns = list(
                set(columns)
                .intersection(self.data.columns)
                )
        return columns

    #/************************************************************************/
    def _clean_place(self, **kwargs):
        self._clean_cols('place', **kwargs)

    #/************************************************************************/
    def locate_data(self, *latlon, **kwargs):
        """Retrieve the geographical coordinates, may that be from existing lat/lon
        columns in the source file, or by geocoding the location name.

            >>> datnat.locate_data(latlon=['lat', 'lon'], **kwargs)
        """
        latlon = (latlon not in ((None,),()) and latlon)                    \
            or kwargs.pop('latlon', None)
        if not isinstance(latlon, string_types) and isinstance(latlon, Sequence):
            if isinstance(latlon, Sequence) and len(latlon) == 1:
                latlon = latlon[0]
        if isinstance(latlon, string_types):
            lat = lon = latlon
        elif isinstance(latlon, Sequence):
            lat, lon = latlon
        elif not latlon in ([],None):
            raise TypeError("Wrong LAT/LON fields - must be a single or a pair of string(s)")
        place = kwargs.pop('place', self.meta.get('place'))                 \
            or DEF_PLACE # list of fields used to define a place
        try:
            assert isinstance(place, (Mapping,Sequence))
        except AssertionError:
            raise TypeError("Wrong type of PLACE columns name(s) '%s'" % place)
        order = kwargs.pop('order', 'lL')
        if latlon in ([],None):
            lat, lon = self.idx.get('lat', 'lat'), self.idx.get('lon', 'lon')
            order = 'lL'
        # lang = kwargs.pop('lang', self.lang)
        oindex = self.config.get('index',{})
        oopts = self.config.get('options',{})
        oproj = kwargs.pop('proj', oopts.get('proj'))
        oplace = oopts.get('place') or 'place'
        opts_locate = self.get_options(opts = kwargs, process = 'locate')
        try:
            olat, olon = oindex['lat']['name'], oindex['lon']['name']
            otlat, otlon = oindex['lat']['type'], oindex['lon']['type']
        except:
            olat, olon = 'lat', 'lon'
            otlat, otlon = None, None
        if lat == lon and lat in self.data.columns: #self.columns[lang]
            latlon = lat
            if order == 'lL':
                lat, lon = olat, olon
            elif order == 'Ll':
                lat, lon = olon, olat
            else:   raise IOError("Unknown order keyword - must be 'lL' or 'Ll'")
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
            try:
                assert oplace in self.data.columns
            except:
                place = self._match_cols(place, force = True)
                place = list(
                    set(list(place.values()))
                    .intersection(self.data.columns)
                    )
                try:    assert place != []
                except: raise IOError("No PLACE column(s) to be used for geolocation found in dataset")
                self.data[oplace] = (
                    self.data[place]
                    .astype(str)
                    .apply(', '.join, axis=1)
                    )
            try:
                assert 'place' in self.idx.keys() and 'place' in oindex.keys()
            except:
                self.idx.update({'place': oplace})
            f = lambda place : self.geoserv.locate(place)
            try:                        f(coder=-1)
            except TypeError:
                self.data[olat], self.data[olon] = \
                    zip(*self.data[oplace].apply(f))
                self.proj = None
            except ImportError:
                raise IOError("No geocoder available")
            geo_qual = None # TBD
        try:
            oqual = oindex.get('geo_qual',{}).get('name') or 'geo_qual'
            self.data[oqual] = geo_qual
        except:
            pass
        else:
            try:
                assert 'geo_qual' in self.idx.keys() and 'geo_qual' in oindex.keys()
            except:
                self.idx.update({'geo_qual': oqual})
        # update
        # no need: self.columns.extend([{'en': ind}])
        # no need: self.columns.extend([{'en': olat}, {'en': olon}}])
        if oproj is not None and self.proj not in (None,'') and self.proj != oproj:
            f = lambda l, L :                                               \
                self.geoserv.project([l, L], iproj = self.proj, oproj = oproj, **opts_locate)
            try:                        f('-1')
            except TypeError:
                self.data[olat], self.data[olon] = zip(*self.data[[olat, olon]].apply(f))
            except ImportError:
                raise IOError("No projection transformer available")
        # cast
        # self.data[olat], self.data[olon] = pd.to_numeric(self.data[olat]), pd.to_numeric(self.data[olon])
        try:
            self.data[olat], self.data[olon] =                              \
                self.data[olat].astype(Type.name2pyt(otlat)),                \
                self.data[olon].astype(Type.name2pyt(otlon))
        except:
            pass
        else:
            try:
                assert ('lat' in self.idx.keys() and 'lon' in self.idx.keys())  \
                    and ('lat' in oindex.keys() and 'lon' in oindex.keys())
            except:
                self.idx.update({'lat': olat, 'lon': olon})

    #/************************************************************************/
    def format_data(self, *index, **kwargs):
        """Run the formatting of the input data according to the harmonised template
        as provided by the index metadata.

            >>> datnat.format_data(*index, **kwargs)
        """
        index = (index not in ((None,),()) and columns[0])  \
                    or kwargs.pop('index',{})
        if isinstance(index, string_types):
            index = {index: None}
        elif isinstance(index, Sequence):
            index =  {i:i for i in index} # dict(zip(index, index))
        elif not isinstance(index, Mapping):
            raise TypeError("Wrong format for input INDEX - must a mapping dictionary")
        force_cols = kwargs.pop('force', False)
        if not isinstance(force_cols, bool):
           raise TypeError("Wrong input format for FORCE parameter - must be a bool")
        keep_cols = kwargs.pop('keep', None)
        if keep_cols is None:
            pass
        elif isinstance(keep_cols, string_types):
            keep_cols = [keep_cols,]
        elif isinstance(keep_cols, bool):
            if keep_cols is False: keep_cols = []
        elif (isinstance(keep_cols, Sequence)                              \
              and all([isinstance(col,string_types) for col in keep_cols])):
            raise TypeError("Wrong input format for KEEP columns - must be a (list of) string(s)")
        idtfmt = kwargs.pop('dtfmt', None)
        #lang = opts_format.get('lang') or DEF_LANG
        #if not isinstance(lang, string_types):
        #    raise TypeError("Wrong format for language - must a string")
        try:
            columns = self.idx.copy()
            columns.update(index) # index overwrites whatever is in idx
        except:
            raise IOError("No index available for formatting")
        # check for country- and date-related columns - special cases
        oindex = self.config.get('index', {})
        for _col in ['country', 'cc', 'date']:
            col = columns.get(_col) or _col
            if _col not in oindex.keys() or col in self.data.columns:
                continue
            self.data[col] = getattr(self, _col, np.nan)
            columns.update({_col: col})
            try:
                assert _col in self.idx.keys() # and already: _col in oindex.keys()
            except:
                self.idx.update({_col: col})
        opts_format = self.get_options(opts = kwargs, process = 'format')
        odtfmt = opts_format.get('dtfmt') or ''
        idtfmt = idtfmt or odtfmt
        # set the columns with the right exptected names
        columns = self._match_cols(cols = columns, **opts_format)
        if columns == {}:
            logging.warning("No columns to be formatted")
            return
        #columns = {k:oindex[v].get('name', v) if v in oindex.keys() else v
        #                for (k,v) in columns.items()}
        indexes = list(self.idx.keys())
        inicols = list(self.data.columns)
        col2ind = {}
        for ind in indexes:
            try:
                col = oindex[ind].get('name')
                assert col is not None and col in inicols
            except:
                try:
                    col = columns.get(ind)
                    assert col is not None and col in inicols
                except:
                    logging.warning("No matching column found for '%s' attribute" % ind)
                    continue
            col = col2ind.get(col, col)
            try: #if ind in oindex.keys():
                ind = oindex[ind].get('name') or ind
                typ = oindex[ind].get('type')
            except:
                try:
                    typ = [o.get('type') for o in oindex.values() if ind == o.get('name')][0]
                except:
                    typ = None
            if ind in self.data.columns or col in self.data.columns: # not inicols!
                try:
                    self.data[ind] = self.data[col]
                except:
                    self.data[ind] = np.nan
            else:
                col2ind.update({col: ind})
                self.data.rename(columns = {col: ind}, inplace = True,
                                 errors = 'ignore')
            try:
                cast = Type.name2pyt(typ)
            except:
                continue
            if cast == self.data[ind].dtype:
                continue
            elif cast == datetime:
                self.data[ind] = Frame.cast(self.data, ind, odfmt = odtfmt, idfmt = idtfmt)
            else:
                self.data[ind] = Frame.cast(self.data, ind, cast)
            try:
                assert ind in oindex.keys() and ind in self.idx.keys()
            except:
                self.idx.update({ind: col})
        # clean the data so that it matches the template; keep even those fields
        # from index which have no corresponding column
        # options:
        if keep_cols is None:
            keep_cols = list(columns.keys())
        elif keep_cols is True:
            #keepcols = [oindex[k]['name'] for (k,v) in self.idx.items()    \
            #            if k in oindex.keys() and v is not None]
            #keepcols = list(self.idx.keys())
            keep_cols = [val['name'] for val in oindex.values()]
        self._clean_cols(list(self.data.columns), keep = keep_cols)
        if force_cols is False:
            return
        # 'keep' the others, i.e. when they dont exist create with NaN
        for ind in keep_cols:
            if ind in self.data.columns:
                continue
            cast = Type.name2pyt(oindex[ind]['type']) if ind in oindex.keys() else object
            if cast == datetime:    cast = str
            try:
                self.data[ind] = pd.Series(dtype=cast)
            except:     pass

    #/************************************************************************/
    def _dump_data(self, **kwargs):
        """Return JSON or GEOJSON formatted data.

            >>> dic = datnat._dump_data(fmt='json')
            >>> geom = datnat._dump_data(fmt='geojson')
        """
        fmt = kwargs.pop('fmt', None)
        if fmt is None: # we give it a default value...
            fmt = 'json'
        elif not isinstance(fmt, string_types):
            raise TypeError("Wrong input format - must be a string key")
        else:
            fmt = fmt.lower()
        formats = self.config.get('options',{}).get('fmt') or {}
        if not fmt in formats:
            raise IOError("Wrong input format - must be any string among '%s'" % list(formats.keys()))
        elif fmt in ('csv','gpkg'):
            raise IOError("format '%s' not supported" % fmt)
        oindex = self.config.get('index',{})
        columns = kwargs.pop('cols', None) or [ind['name'] for ind in oindex.values()]
        latlon = kwargs.pop('latlon', None) or [oindex['lat']['name'], oindex['lon']['name']]
        if fmt == 'geojson':
            try:
                results = Frame.to_geojson(self.data, columns = columns, latlon = latlon)
            except:     raise IOError("Issue when creating GEOJSON geometries")
        elif  fmt == 'json':
            try:
                results = Frame.to_json(self.data, columns = columns)
            except:     raise IOError("Issue when creating JSON attributes")
        try:
            assert kwargs.pop('as_str', False) is False
        except:
            opts_dump = self.get_options(opts = kwargs, process = 'dump')
            opts_dump.update({'ensure_ascii': opts_dump.pop('ensure_ascii', False)})
            try:
                return Json.dumps(results, **opts_dump)
            except:     raise IOError("issue when dumping '%s' attributes" % fmt.upper())
        else:
            return results

    #/************************************************************************/
    def save_data(self, *dest, **kwargs):
        """Store transformed data in GEOJSON or CSV formats.

            >>> datnat.save_data(dest=filename, fmt='csv')
            >>> datnat.save_data(dest=filename, fmt='geojson')
        """
        dest = (dest not in ((None,),()) and dest[0])                       \
            or kwargs.pop('dest', None)
        fmt = kwargs.pop('fmt', None)
        # first consider the generic options, then the country-specific as well
        # as the locally parsed options that supersede/override them
        opts_save = self.get_options(opts = kwargs, process = 'save')
        formats = opts_save.pop('fmt', None) or {f:f for f in DEF_FORMATS}
        if fmt is None: # we give it a default value...
            try:
                fmt = FileSys.extname(dest)
                assert fmt != ''
            except:
                fmt = list(formats.keys())[0] or DEF_FORMAT
        elif not isinstance(fmt, string_types):
            raise TypeError("Wrong input format - must be a string key")
        fmt = fmt.lower()
        try:
            assert fmt in formats.values()
        except:
            try:
                assert fmt in formats.keys()
            except:
                raise TypeError("Wrong input format - must be any string among '%s'" % list(formats.keys()))
            else:
                fmt = formats[fmt]
        if dest in (None,''):
            destpath = self.config.get('path', './')
            destname = self.config.get('file', None)
            try:
                assert len(re.findall('%s', destname)) == 2
            except:
                destname = '%s.%s'
            finally:
                destname = destname % (self.cc, fmt)
            dest = osp.abspath(osp.join(destpath, fmt, destname))
            logging.warning("\n! Output data file '%s.%s' will be created" %
                            (FileSys.basename(dest),fmt))
        oindex = self.config.get('index', {})
        columns = kwargs.pop('cols', None)
        if columns is True:
            columns = self.data.columns
        elif columns in (None,[]):
            try:
                columns = [ind['name'] for ind in oindex.values()]
            except:     raise IOError("Input columns not set")
        latlon = kwargs.pop('latlon', None)
        if latlon is True:
            try:
                latlon = self.idx['lat'], self.idx['lon']
            except:     raise IOError("Geographic LATLON columns not set")
        elif latlon in (None,[]):
            try:
                latlon = oindex['lat']['name'], oindex['lon']['name']
            except:     raise IOError("Geographic LATLON columns not set")
        self.data.reindex(columns = columns)
        columns = list(set(columns)
                       .intersection(set(self.data.columns))
                       )
        try:
            assert columns not in (None,[])
        except:
            logging.warning("No COLUMNS parsed to the saving operation")
            return
        opts_save.update({'fmt': fmt, 'columns': columns})
        if fmt == 'csv':
            opts_save.update({'header': True, 'index': False})
        elif fmt in ('json','geojson','gpkg'):
            opts_save.update({'as_str': False, 'latlon': latlon})
        Frame.to_file(self.data, dest, **opts_save)

    #/************************************************************************/
    def _dump_config(self, **kwargs):
        logging.warning("\n! Method not implemented !")
        return

    #/************************************************************************/
    def save_config(self, *dest, **kwargs):
        self.config.dump(*dest, **kwargs)

    #/************************************************************************/
    def get_meta(self, keys = None, **kwargs):
        force = kwargs.pop('force', False)
        meta = self.meta.to_dict() # self.meta.__dict__
        if keys is True:
            keys = MetaDatNat.PROPERTIES
        elif keys is None:
            try:
                keys = meta.PROPERTIES
            except:
                keys = meta.keys()
            # keys = list(set(keys)).intersection(set(MetaDatNat.PROPERTIES))
        for attr in keys:
            if attr in ['index', 'columns']:
                if force is False:
                    continue
                meta.update({attr:
                             deepcopy(getattr(self, {'index': 'idx', 'columns': 'cols'}[attr]))
                             })
            elif attr == 'country':
                meta.update({'country': isoCountry(self.cc)})
            elif attr == 'lang':
                meta.update({'lang': isoLang(self.lang)})
            else:
                try:
                    meta.update({attr: getattr(self,attr)})
                except:         pass
        # meta.update({'options': self.options})
        return meta

    #/************************************************************************/
    def update_meta(self):
        """Update the metadata file.
        """
        self.meta.update(self.get_meta(keys = MetaDatNat.PROPERTIES, force = True))

    #/************************************************************************/
    def _dump_meta(self, **kwargs):
        """Dump metadata as output JSON dictionary.

            >>> meta = fac._dump_data()
        """# basically... nothing much more than self.meta.to_dict()
        if _IS_META_UPDATED is False:
            meta = self.get_meta(**kwargs)
        else:
            meta = self.meta.to_dict()
        if kwargs.pop('as_str', False) is False:
            return meta
        kwargs.update({'ensure_ascii': kwargs.pop('ensure_ascii', False)})
        # kwargs = Object.inspect_kwargs(kwargs, Json.dump)
        try:
            return Json.dumps(meta, **kwargs) # pprint.pformat(meta)
        except:     raise IOError("Impossible dumping metadata file")

    #/************************************************************************/
    def save_meta(self, *dest, **kwargs):
        """Dump metadata into a JSON file.

            >>> datnat.save_meta(dest=metaname)
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
                dest = osp.join(PACKPATH, self.category.lower(), '%s%s.json'
                                % (self.cc.upper(), self.category.lower()))
            except:
                dest = osp.join(PACKPATH, '%s.json' % self.cc)
            logging.warning("\n! Metadata file '%s.%s' will be created" %
                            (FileSys.basename(dest),fmt))
        if _IS_META_UPDATED is False:
            meta = self.get_meta(**kwargs)
        else:
            meta = self.meta.to_dict()
        kwargs.update({'ensure_ascii': kwargs.pop('ensure_ascii', False)})
        # kwargs = Object.inspect_kwargs(kwargs, Json.dump)
        with open(dest, 'w', encoding='utf-8') as f:
            try:
                Json.dump(meta, f, **kwargs)
            except:     raise IOError("Impossible saving metadata file")
        return

#==============================================================================
# Function datnatFactory
#==============================================================================

def datnatFactory(*args, **kwargs):
    """Generic function to derive a class from the base class :class:`BaseFacility`
    depending on specific metadata and a given geocoder.

        >>>  NewNatDat = datnatFactory(config = None, meta = None,
                                       country = None, coder = None)

    Examples
    --------

        >>>  NewHCS = datnatFactory(HCS, country=CC1, coder={'Bing', yourkey})
        >>>  NewFacility = datnatFactory(country=CC2, coder='GISCO')
    """
    basecls = BaseDatNat # kwargs.pop('base', BaseDatNat)
    attributes = {}
    # check facility to define output data configuration format
    if args in ((),(None,)):        config = None
    else:                           config = args[0]
    config = config or kwargs.pop('config', None)
    try:
        assert config is None or isinstance(config, (Mapping,MetaDat))
    except AssertionError:
        raise TypeError("Configuration type '%s' not recognised - must be a dictionary or %s" % (type(config),MetaDat.__name__))
    if config is None or isinstance(config, MetaDat):
        pass
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
    #elif isinstance(config, MetaDat):
    #    try:
    #        config = config.copy()
    #    except:
    #        raise IOError("Configuration metadata '%s' not recognised" % str(config))
    elif isinstance(config, Mapping):
        try:
            config = MetaDat(config)
        except:     raise IOError("Configuration dictionary '%s' not recognised" % config)
    try:
        CATEGORY = config.category if hasattr(config, 'category') else config.get("category")
        assert CATEGORY is None or isinstance(CATEGORY,(Mapping,string_types))
    except:
        CATEGORY = None
    else:
        attributes.update({'CATEGORY': CATEGORY})
    # check options of input data
    try:
        oopts = config.get('options') if config is not None and 'options' in config \
            else kwargs.pop('options', None)
        assert oopts is None or isinstance(oopts,Mapping)
    except AssertionError:
        raise TypeError("Options type '%s' not recognised - must be a dictionary" % type(oopts))
    # check metadata of input data
    meta = kwargs.pop('meta', None)
    try:
        assert meta is None or isinstance(meta,(string_types,Mapping,MetaDatNat))
    except AssertionError:
        raise TypeError("Metadata type '%s' not recognised - must be a filename, dictionary or %s" % (type(meta),MetaDatNat.__name__))
    if meta is None:
        pass # meta = None # meta = MetaDatNat({})
    elif isinstance(meta,MetaDatNat):
        try:
            meta = meta.copy()
        except:     raise IOError("Metadata '%s' not recognised "  % meta)
    elif isinstance(meta, (string_types, Mapping)):
        try:
            meta = MetaDatNat(meta)
        except:     raise IOError("Metadata '%s' not recognised " % meta)
    # check country
    try:
        COUNTRY = meta.get('country') if meta is not None and 'country' in meta \
            else kwargs.pop('country', None)
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
        VERS = meta.get('version') if meta is not None and 'version' in meta else kwargs.pop('version', None)
        assert VERS is None or isinstance(VERS,int) or isinstance(VERS,datetime)
    except AssertionError:
        raise IOError("Version type '%s' not recognised - must be an integer or a date" % type(VERS))
    else:
        attributes.update({'VERSION': VERS})
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
            geocoder = DEF_CODER
        except ImportError:
            logging.warning("\n! No geocoder available !")
            geocoder = None
        except:     raise IOError("Geocoder '%s' not recognised " % CODER)
    else:
        geocoder = None
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
            self.geocoder = geocoder
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
        name = '%s%s' % (CC.upper(), CATEGORY['code'].lower())
    except:
        name = 'New%s' % basecls.__name__.replace('Base','')
    return type(name, (basecls,), attributes)

