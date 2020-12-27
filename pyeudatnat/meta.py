#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.. _meta

Module metadata template models.

**Dependencies**

*require*:      :mod:`os`, :mod:`six`, :mod:`collections`, :mod:`functools`, :mod:`copy`

*call*:         :mod:`pyeudatnat`, :mod:`pyeudatnat.io`

**Note**

Consider using `pydantic` package: https://github.com/samuelcolvin/pydantic/.

**Contents**
"""

# *credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_
# *since*:        Thu Apr  9 09:56:45 2020

#%% Settings

from os import path as osp
import pprint, logging

from collections import OrderedDict
from collections.abc import Mapping, Sequence
from six import string_types

from functools import reduce
from copy import deepcopy

from pyeudatnat import COUNTRIES#analysis:ignore
from pyeudatnat.io import File, Json#analysis:ignore


#==============================================================================
#%% Class MetaDat

class MetaDat(dict):
    """Generic class used to represent metadata instances as dictionary.

        >>> meta = MetaDat(**metadata)
    """

    CATEGORY = 'UNK'
    PROPERTIES = []

    #/************************************************************************/
    def __init__(self, *args, **kwargs):
        if not args in ((),(None,)):
            try:
                meta = deepcopy(args[0]) # deepcopy to avoid updating the default variables!!!
            except:
                meta = args[0]
        else:
            meta = {}
        if isinstance(meta, MetaDat):
            meta = dict(meta).copy()
        elif isinstance(meta, string_types):
            if not osp.exists(meta):
                raise IOError("Input metadata filename '%s' not recognised" % meta)
            with open(meta, 'rt') as fp:
                try:
                    meta = Json.load(fp, serialize=kwargs.pop('serialize',False))
                except:
                    raise IOError("Input metadata file '%s' must be in JSON format" % meta)
        elif not isinstance(meta, Mapping):
            raise TypeError("Input metadata format '%s' not recognised - must be a mapping dictionary or a string" % type(meta))
        meta.update(kwargs)
        super(MetaDat,self).__init__(**meta)
        self.__dict__ = self

    #/************************************************************************/
    @property
    def category(self):
        try:
            assert 'category' in self.__dict__
            return self.get('category') # if 'category' in self.__dict__
        except:
             return self.CATEGORY

    #/************************************************************************/
    def copy(self, *args, **kwargs): # actually new object, like a deepcopy...
        #return self.__class__(self.__dict__) # would work as well, see __init__
        return self.__class__(**self.__dict__)

    #/************************************************************************/
    def to_dict(self): # actually new object, like a deepcopy...
        d = dict(**self.__dict__) # deepcopy(self.__dict__)
        keys = list(d.keys())
        [d.pop(key) for key in keys if key.startswith('__') or key.startswith('_' + self.__class__.__name__)]
        return d

    #/************************************************************************/
    def __repr__(self):
        return "<{} metadata instance at {}>".format(self.__class__.__name__, id(self))
    def __str__(self):
        d = self.to_dict()
        if d == []:
            return " "
        #l = max([len(k) for k in d.keys()])
        #return reduce(lambda x,y:x+y, ["{} : {}\n".format(k.ljust(l),getattr(self,k))
        #    for k in d.keys() if self.get(k) not in ('',None)])
        return pprint.pformat(d)

    #/************************************************************************/
    def __getattr__(self, attr):
        if attr.startswith('__'):
            try:        nattr = attr[2:-2]
            except:     nattr = attr
        else:
            nattr = attr
        if nattr in self.keys():
            r = self.get(nattr)
        else:
            try:        object.__getattribute__(self, attr)
            except:     pass
            r = None
        return r

    #/************************************************************************/
    def load(self, src=None, **kwargs):
        if src is None:
            # raise IOError("no source metadata file defined")
            try:        cat = self.category.get('code')
            except:     cat = self.category
            src = osp.join("%s.json" % cat)
        elif not isinstance(src, string_types):
            raise TypeError("Wrong source metadata file '%s'" % src)
        try:
            assert osp.exists(src)
        except AssertionError:
            raise IOError("Metadata file '%s' do not exist" % src)
        with open(src, 'r') as fp:
            try:
                meta = Json.load(fp, **kwargs)
            except:
                raise IOError("Error saving metadata file")
        if 'category' not in meta.keys() and self.category not in (self.CATEGORY,None):
            meta.update({'category': self.category})
        return meta

    #/************************************************************************/
    def loads(self, src=''):
        logging.warning("\n! Method 'loads' not implemented")
        pass

    #/************************************************************************/
    def update(self, arg, **kwargs):
        if isinstance(arg, string_types):
            meta = self.load(src = arg, **kwargs)
        elif isinstance(arg, (Mapping,MetaDat)):
            meta = deepcopy(arg) # arg.copy()
        else:
            raise TypeError("Wrong type for input metadata parameter '%s'" % arg)
        try:
            assert meta != {}
        except:
            # logging.warning("\n! empty metadata !")
            return
            # raise IOError('no metadata variable loaded')
        else:
            # logging.warning("\n! loading metadata !")
            pass
        if kwargs.pop('keep', True) is False:
            [self.pop(key) for key in self.keys()]
        #keys = self.keys() or list(meta.keys())
        #if set(keys).intersection(set(list(meta.keys()))) == set():
        #    return
        #super(MetaDat,self).update({var: meta.get(var) for var in keys if var in meta})
        super(MetaDat,self).update(meta)

    #/************************************************************************/
    def dumps(self, **kwargs):
        meta = {k:v for (k,v) in dict(self.copy()).items() if k in self.keys()}
        if meta == {}:
            raise IOError("No metadata variable available")
        try:
            return Json.dumps(meta, **kwargs)
        except:
            raise IOError("Error dumping metadata file")

    #/************************************************************************/
    def dump(self, dest=None, **kwargs):
        if dest is None:
            # raise IOError("no destination metadata file defined")
            try:        cat = self.category.get('code')
            except:     cat = self.category
            dest = osp.join("%s.json" % cat)
        elif not isinstance(dest, string_types):
            raise TypeError("Wrong destination metadata file '%s'" % dest)
        try:
            assert osp.exists(dest)
        except AssertionError:
            logging.warning('\n! Destination metadata file will be created !')
        else:
            logging.warning('\n! Destination metadata file will be overwritten !')
        meta = {k:v for (k,v) in dict(self.copy()).items() if k in self.keys()}
        if meta == {}:
            raise IOError("No metadata variable available")
        with open(dest, 'w') as fp:
            try:
                Json.dump(meta, fp, **kwargs)
            except:
                raise IOError("Error saving metadata file")


#==============================================================================
#%% Class MetaDatNat

class MetaDatNat(MetaDat):
    """Metadata class for national datasets.
    """

    PROPERTIES = [ 'provider', 'country', 'file', 'path', 'columns',
                   'index', 'options', 'category', 'date' ]

    #/************************************************************************/
    def __init__(self, *args, **kwargs):
        super(MetaDatNat,self).__init__(*args, **kwargs)
        # self.__dict__ = self
        try:
            self.cc = self['country'].get('code','')
        except:
            self.cc = None

    #/************************************************************************/
    @property
    def cc(self):
        return self.__cc # or ''
    @cc.setter#analysis:ignore
    def cc(self, cc):
        if cc is None:                          pass
        elif not isinstance(cc, string_types):
            raise TypeError("Wrong format for country code '%s' - must be a string" % cc)
        elif not cc in COUNTRIES: # COUNTRIES.keys()
            raise IOError("Wrong country code '%s' - must be any valid code from the EU area" % cc)
        self.__cc = cc

    #/************************************************************************/
    def to_dict(self): # actually new object, like a deepcopy...
        d = dict(**self.__dict__)
        keys = list(d.keys())
        [d.pop(key) for key in keys if key.startswith('__') or key.startswith('_MetaDatNat__')]
        return d

    #/************************************************************************/
    def load(self, src=None, **kwargs):
        if src is None:
            try:
                # src = osp.join(PACKPATH, self.type.lower(), "%s%s.json" % (self.cc.upper(),self.type.lower()))
                src = osp.join(PACKPATH, "%s%s.json" % (self.cc.upper(),self.type.lower()))
            except:
                raise IOError('No source metadata file defined')
        return super(MetaDatNat,self).load(src=src, **kwargs)

    #/************************************************************************/
    def dump(self, dest=None, **kwargs):
        if dest is None:
            try:
                # dest = osp.join(PACKPATH, self.type.lower(), "%s%s.json" % (self.cc.upper(),self.type.lower()))
                dest = osp.join(PACKPATH, "%s%s.json" % (self.cc.upper(),self.type.lower()))
            except:
                raise IOError('No destination metadata file defined')
        super(MetaDatNat,self).dump(dest=dest, **kwargs)

