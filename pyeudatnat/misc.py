#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.. _misc

Module implementing miscellaneous useful methods.

**Dependencies**

*require*:      :mod:`os`, :mod:`six`, :mod:`collections`, :mod:`inspect`, :mod:`re`, 
                :mod:`numpy`, :mod:`datetime`, :mod:`time`, :mod:`operator`

*optional*:     :mod:`dateutil`, :mod:`zipfile`

*call*:         :mod:`pyeudatnat`         

**Contents**
"""

# *credits*:      `gjacopo <gjacopo@ec.europa.eu>`_ 
# *since*:        Sun Apr 19 16:36:19 2020


#%% Settings

import io, os, sys#analysis:ignore
from os import path as osp
import inspect
import re
import warnings#analysis:ignore

import operator
from collections import OrderedDict, Mapping, Sequence#analysis:ignore
from six import string_types

import time
import datetime 
import calendar
try:    
    import dateutil
except ImportError:
    _is_dateutil_installed = False
    try:
        import pytz  
        # warnings.warn('pytz help: https://pypi.python.org/pypi/pytz')   
    except: pass
else:
    _is_dateutil_installed = True
    # warnings.warn('dateutil help: https://pypi.python.org/pypi/python-dateutil')   
    try:    assert dateutil.parser # issue with IPython
    except: 
        import dateutil.parser 

import numpy as np

try:
    import zipfile
except:
    _is_zipfile_installed = False
else:
    _is_zipfile_installed = True

from pyeudatnat import PACKNAME


#%% Global var

ISWIN           = os.name=='nt' # sys.platform[0:3].lower()=='win'


#==============================================================================
#%% Class Miscellaneous

class Miscellaneous(object):
    
    #/************************************************************************/
    @staticmethod
    def inspect_kwargs(kwargs, method):
        """Clean keyword parameters prior to be passed to a given method/function by
        deleting all the keys that are not present in the signature of the method/function.
        """
        if kwargs == {}: return {}
        kw = kwargs.copy() # deepcopy(kwargs) 
        parameters = inspect.signature(method).parameters
        keys = [key for key in kwargs.keys()                                          \
                if key not in list(parameters.keys()) or parameters[key].KEYWORD_ONLY.value==0]
        [kw.pop(key) for key in keys]
        return kw


#==============================================================================
#%% Class Type

class Type(object):   
    
    # Variables of useful types conversions
    __PYTYPES = {'object':'object', \
                 'int':'uint8', 'uint8':'uint8', 'uint16':'uint16', 'int16':'int16',    \
                 'long': 'uint32', 'uint32':'uint32', 'int32':'int32',                  \
                 'float':'float32', 'float32':'float32', 'float64':'float64'            \
                 }
    
    # Python pack types names
    __PPTYPES = ['B', 'b', 'H', 'h', 'I', 'i', 'f', 'd'] # personal selection
    #=========   ==============  =================   =====================
    #Type code   C Type          Python Type         Minimum size in bytes
    #=========   ==============  =================   =====================
    #'c'         char            character           1
    #'u'         Py_UNICODE      Unicode character   2
    #'B'         unsigned char   int                 1
    #'b'         signed char     int                 1
    #'H'         unsigned short  int                 2
    #'h'         signed short    int                 2
    #'I'         unsigned int    long                2
    #'i'         signed int      int                 2
    #'L'         unsigned long   long                4
    #'l'         signed long     int                 4
    #'f'         float           float               4
    #'d'         double          float               8
    #=========   ==============  =================   =====================    
    #See http://www.python.org/doc//current/library/struct.html and
    #http://docs.python.org/2/library/array.html.
    
    # NumPy types names
    __NPTYPES         = [np.dtype(n).name for n in __PPTYPES+['l','L','c']]
    
    # Pandas types names
    __PDTYPES         = [np.dtype(n).name for n in ['b', 'i', 'f', 'O', 'S', 'U', 'V' ]]
    #=========   ==============  
    #Type code   C Type
    #=========   ==============  
    #'b'         boolean
    #'i'         (signed) integer
    #'u'         unsigned integer
    #'f'         floating-point
    #'c'         complex-floating point
    #'O'         (Python) objects
    #'S', 'a'    (byte-)string
    #'U'         Unicode
    #'V'         raw data (void)

    # Dictionary of Python pack types -> Numpy
    __PPT2NPT = {n:np.dtype(n).name for n in __PPTYPES}
    # See http://docs.scipy.org/doc/numpy/reference/arrays.dtypes.html and
    # http://docs.scipy.org/doc/numpy/reference/arrays.scalars.html.
    
    # Dictionary of Python -> Numpy
    __NPT2PYT = { np.dtype('b'):                bool,
                  np.dtype('i'):                bool,
                  np.dtype('O'):                str, # not object
                  np.dtype('U'):                str,
                  object:                       object,
                  np.dtype('i'):                int, 
                  np.dtype('uint32'):           int, 
                  np.dtype(int):                int, 
                  np.dtype('f'):                float, 
                  np.dtype(float):              float,
                  np.dtype('datetime64'):       datetime.datetime,
                  np.dtype('datetime64[ns]'):   datetime.datetime
                 } 
  
    # Dictionary of Python -> Numpy
    def __rev_dict_unique_values(d):
        dd = {}
        [dd.setdefault(v, []).append(k) for (k,v) in d.items()]  
        return dd 
    __PYT2NPT = __rev_dict_unique_values(__NPT2PYT)        
    #__PY2NPT = { bool:      [np.dtype('b'), np.dtype('i')],
    #             str:        [np.dtype('O'), object], 
    #             int:        [np.dtype('i'), np.dtype('uint32'), np.dtype(int)],
    #             float:      [np.dtype('f'), np.dtype(float)],
    #             datetime.datetime:   [np.dtype('datetime64'), np.dtype('datetime64[ns]')],
    #             } 

    #/************************************************************************/
    ppt2npt = lambda t: Type.__PPT2NPT[t]
    """Conversion method Python pack <-> Numpy types.
    """
    # note regarding np.dtype:
    #   np.dtype('B')           -> dtype('uint8')
    #   np.dtype('uint8')       -> dtype('uint8')
    #   np.dtype(np.uint8)      -> dtype('uint8')
    # so that on the current machine where it is implemented
    # assert ppt2npy == {'B':'uint8','b': 'int8','H':'uint16','h':'int16','I':'uint32',
    #   'i':'int32', 'f':'float32', 'd':'float64'}
    # but... in the future?!         
    
    #/************************************************************************/
    npt2ppt = lambda t: dict(Type.__PPT2NPT.values(), Type.__PPT2NPT.keys())[t]
    """Conversion method Numpy -> Python pack types.
    """

    #/************************************************************************/
    npt2pyt = lambda t: Type.__NPT2PYT[t]
    """Conversion method Numpy -> Python types.
    """
    
    #/************************************************************************/
    pdt2pyt = npt2pyt
    """Conversion method Pandas -> Python types.
    """
        
    #/************************************************************************/
    pyt2npt = lambda t: Type.__PYT2NPT[t]
    """Conversion method Python -> Numpy types.
    """

    __UPYT2NPT = { bool:                       np.dtype('b'),
                   str:                        np.dtype('U'),
                   int:                        np.dtype(int), #np.dtype('i')
                   float:                      np.dtype(float), # np.dtype('f')
                   datetime.datetime:          np.dtype('datetime64'),
                   object:                     np.dtype('O')
                   } 
        
    #/************************************************************************/
    upyt2npt = lambda t: Type.__UPYT2NPT[t]
    """Conversion method Python -> unique Numpy type.
    """
        
    #/************************************************************************/
    upyt2pdt = upyt2npt
    """Conversion method Python -> unique Pandas type.
    """
    
    #/************************************************************************/
    pyt2pdt = pyt2npt
    """Conversion method Python -> Pandas types.
    """
    
    # Dictionary of Numpy type -> unique Python name
    __NPT2UPYN = {k:v.__name__ for (k,v) in __NPT2PYT.items()}
    
    # Dictionary of Python name -> unique Numpy type
    __UPYN2NPT = {k.__name__:v for (k,v) in __UPYT2NPT.items()}

    upytname2npt = lambda n: Type.__UPYN2NPT[n]
    """Conversion method Python type name -> unique Numpy type.
    """
     
    unpt2pytname = lambda t: Type.__NPT2UPYN[t]
    """Conversion method Numpy type -> unique Python type name.
    """

    upytname2pdt = upytname2npt
    """Conversion method Python type name -> unique Pandas type.
    """
    
    #/************************************************************************/
    pytname2npt = lambda n: {k.__name__:v for (k,v) in Type.__PYT2NPT.items()}[n]
    """Conversion method Python type name -> Numpy types list.
    """
    
    #/************************************************************************/
    pytname2pdt = pytname2npt
    """Conversion method Python type name -> Pandas types list.
    """
   
    
#==============================================================================
#%% Class Datetime
    
class Datetime(object):
    """Static and class methods for datetime objects manipulation.
    """

    try:        
        UTC_TZ  = dateutil.tz.tzutc()
    except:
        try:    UTC_TZ  = pytz.timezone('UTC')
        except: UTC_TZ  = None
    
    try:    LOCAL_TZ    = dateutil.tz.tzlocal()
    except: LOCAL_TZ    = None #  pytz.localize()
    
    DICT_TIMEZ          = {'local': LOCAL_TZ, 'utc': UTC_TZ} 
    __DEF_TIMEZ           = DICT_TIMEZ['local'] # 'utc'
    
    ZERO                = datetime.timedelta(0)
    HOUR                = datetime.timedelta(hours=1)
    __DICT_DURATION       = {'zero': ZERO, 'hour': HOUR}
    
    TODAY               = datetime.date.today # lambda: datetime.date.fromtimestamp(time.time()) 
    NOW                 = datetime.datetime.now
    UTCNOW              = datetime.datetime.utcnow
    TOMORROW            = lambda: Datetime.TODAY() + datetime.timedelta(1)
    IN24HOURS           = lambda: Datetime.NOW() + datetime.timedelta(1)
    # those are functions
    __DICT_NOW            = {'now': NOW, 'utcnow': UTCNOW, 'today': TODAY, 
                             'tomorrow': TOMORROW, 'in24hours': IN24HOURS} 

    __YR_TO     = {'sec':31556940,   'mn':525949,    'hr':8765.81, 'd':365.242,  'm':12,               'y':1 }   
    __MTH_TO    = {'sec':2629739.52, 'mn':43828.992, 'hr':730.484, 'd':30.4368,  'm':1,                'y':1./12}
    __DAY_TO    = {'sec':86400,      'mn':1440,      'hr':24,      'd':1,        'm':1./__MTH_TO['d'],   'y':1./__YR_TO['d']}
    __HR_TO     = {'sec':3600,       'mn':60,        'hr':1,       'd':1./24,    'm':1./__MTH_TO['hr'],  'y':1./__YR_TO['hr']}
    __MN_TO     = {'sec':60,         'mn':1,         'hr':1./60,   'd':1./1440,  'm':1./__MTH_TO['mn'],  'y':1./__YR_TO['mn']}
    __SEC_TO    = {'sec':1,          'mn':1./60,     'hr':1./3600, 'd':1./86400, 'm':1./__MTH_TO['sec'], 'y':1./__YR_TO['sec']}

    UNITS_TO =  {'y': __YR_TO, 'm': __MTH_TO, 'd': __DAY_TO, 
                 'hr': __HR_TO, 'mn': __MN_TO, 'sec': __SEC_TO}
    
    TIMING_UNITS = ['y', 'm', 'd', 'hr', 'mn', 'sec'] # list(__YR_TO.keys())

    DATETIME_KWARGS = { 'y':'year', 'm' :'month', 'd':'day',
                        'hr':'hour', 'mn':'minute', 'sec':'second'}
    __DATETIME_KWARGS_REV = {v:k for (k,v) in DATETIME_KWARGS.items()}
    __DATETIME_ITEMS = list(DATETIME_KWARGS.keys()) + list(DATETIME_KWARGS.values()) 
    
    TIMEDELTA_KWARGS = { 'y':'years', 'm' :'months', 'd':'days',
                         'hr':'hours', 'mn':'minutes', 'sec':'seconds'}
    __TIMEDELTA_KWARGS_REV = {v:k for (k,v) in TIMEDELTA_KWARGS.items()}
    __TIMEDELTA_ITEMS = list(TIMEDELTA_KWARGS.keys()) + list(TIMEDELTA_KWARGS.values()) 

    #/************************************************************************/
    @classmethod
    def units_to(cls, from_, to, time=1.):       
        """Perform simple timing units conversion.
        
            >>> t = Datetime.units_to(from, to, time=1.)
            
        Arguments
        ---------        
        from,to : str
            'origin' and 'final' units: any strings in :literal:`['y', 'm', 'd', 'hr', 'mn', 'sec']` .
        time : float
            timing value to convert.

        Examples
        --------
        >>> Datetime.units_to('mn', 'hr',  time=60) == 1
            True
        >>> Datetime.units_to('sec', 'd',  10) == 10*Datetime.UNITS_TO['sec']['d']
            True
        >>> Datetime.units_to('hr', 'sec',  5) == 5*Datetime.UNITS_TO['hr']['sec']
            True
        """
        if not(from_ in cls.__TIMEDELTA_ITEMS and to in cls.__TIMEDELTA_ITEMS):
            raise TypeError('Timing units not implemented')            
        else:
            if from_ in cls.TIMEDELTA_KWARGS.values():
                from_ = cls.__TIMEDELTA_KWARGS_REV[from_]
            if to in cls.TIMEDELTA_KWARGS.values():
                to = cls.__TIMEDELTA_KWARGS_REV[to]
        return cls.UNITS_TO[from_][to] * time
        
    #/************************************************************************/
    @classmethod
    def convert_time_units(cls, to='mn', **kwargs):
        """Convert composed timing units to a single one.
            
            >>> t = Datetime.convert_time_units(to, **kwargs)
            
        Arguments
        ---------        
        to : str
            desired 'final' unit: any string in :literal:`['y', 'm', 'd', 'hr', 'mn', 'sec'] `; 
            default to :literal:`'mn' `.
            
        Keyword Arguments
        -----------------        
        kwargs : dict
            dictionary of composed times indexed by their unit, which can be any
            string in :literal:`['y', 'm', 'd', 'hr', 'mn', 'sec'] `.

        Note
        ----
        Quantities to convert are passed either as a dictionary or as positional
        arguments:        
        
        Example
        -------
        >>> Datetime.convert_time_units('mn', **{{'hr':1, 'sec':420}}) == 67
            True
        >>> Datetime.convert_time_units('mn', hr=1,  sec=420) == 67
            True
        """
        if not to in cls.__TIMEDELTA_ITEMS:
            raise IOError("Timing unit '%s' not implemented" % to)
        elif to in cls.TIMEDELTA_KWARGS.values():
            to = cls.__DATETIME_KWARGS_REV[to]
        t = 0
        for u in cls.__TIMEDELTA_ITEMS:
            if u in kwargs: t += cls.units_to(u, to, kwargs.get(u))
        return t
                                              
    #/************************************************************************/
    @classmethod
    def datetime(cls, *arg, **kwargs):  # datetime = arg
        """Perform time conversions in between various (not so) standard timing 
        formats: unix timestamp, isoformat, ctime, ...
            
            >>> dt = Datetime.datetime(*dtime, **kwargs)
            
        Arguments
        ---------        
        dtime : datetime.datetime, datetime.date, str, float, dict
            an object specifying a time, e.g. it can be:    
            
            - a :class:`datetime.datetime` (or :class:`datetime.date`) object: 
              :literal:`datetime.datetime(2014, 6, 19, 17, 58, 5)`,
            - an iso-formated (ISO 8601) date: :literal:`"2014-06-19T17:58:05"` or 
              :literal:`"2014-06-19 17:58:05"`,
            - a string date: :literal:`"Thu Jun 19 17:58:05 2014"`,
            - a float representing a unix timestamp date: :literal:`1403193485`,
            - an explicit string date: :literal:`"45d 34hr 2900mn 4m 500sec 2y"` 
              (where order does not matter),
            - a dictionary-like date: :literal:`{{'y':2, 'm':4, 'd':45, 'hr':34, 'mn':2900, 'sec':500}}`.               
                
            When the date is expressed as an 'explicit string' or a dictionary, a time unit
            is any string in :literal:`['y', 'm', 'd', 'hr', 'mn', 'sec'] `.
            
            'now', 'utcnow' and 'today' are also accepted instead of an explicit date.
            
        Keyword Arguments
        -----------------   
        fmt : str
            variable specifying the desired output timing format: it can be any string
            among:            
            
            - 'datetime' to output a :class:`datetime.datetime` object,
            - 'dict' to output a dictionary indexed by the timing units, where a key   
              is any string in :literal:`['y', 'm', 'd', 'hr', 'mn', 'sec'] ` 
              (see also :meth:`datetime.timetuple` method)),
            - 'timestamp' to output a unix timestamp (see also :meth:`calendar.timegm` 
              method)),
            - 'iso' to output an iso-formated string date (see also
              :meth:`datetime.isoformat` method),
            - 'calendar' to output a calendar (ISO year, week number, and weekday) date 
              (see also :meth:`datetime.isocalendar` method),
            
            as well as: 
            
            - a date formating string (e.g. :literal:`'%Y-%m-%dT%H:%M:%S'`, see :meth:`datetime.strftime`),
            - :literal:`True` to force a string naming (see :meth:`time.ctime` method)).
            
            When nothing is passed, a default output format is considered, depending 
            on the input format of `dtime` (e.g. a :class:`datetime` object is output for a
            :class:`dict` `dtime`, ...).
        no_micro_secs : bool
            set to :literal:`False` to display microseconds as part of the output datetime when the
            chosen output format is the isoformat; default: :literal:`True`.
            
        Return
        ------
        dt : datetime.datetime, str, float, dict, calendar (tuple)
            a well-formatted (according to `fmt` specification) object 
            representing a datetime equivalent to the input `dtime`.

        Note
        ----
        As for the :class:`dict` format of the input :literal:`dtime`, long time unit names (keys in 
        :literal:`['years', 'months', 'days', 'hours', 'minutes', 'seconds']`) are also accepted.
        
        As for the `timestamp` format, a default timezone is automatically attached 
        to the datetime.
            
        Example
        -------
        >>> import datetime
        
        Let's construct some datetime objects:        
        
        >>> dt = datetime.datetime(2014, 6, 19, 17, 58, 5)
        >>> dt_dict = {{'y':2014, 'm':6, 'd':19, 'hr':17, 'mn':58,  'sec':5}}

        Again, we can use dictionary of positional arguments to pass the proper format:

        >>> dt == Datetime.datetime(dt_dict, fmt='datetime')        
            True
        >>> dt == Datetime.datetime("Thu Jun 19 17:58:05 2014", fmt='datetime')
            True

        By default, it behaves 'well' in many circumstances:
         
        >>> dt == Datetime.datetime(dt_dict)
            True
        >>> Datetime.datetime(dt_dict) == Datetime.datetime(**dt_dict)
            True
        
        However:
        
        >>> Datetime.datetime(dt, fmt='dict')
            {{'second': 5, 'hour': 17, 'year': 2014, 'day': 19, 'minute': 58, 'month': 6}}
            
        Many more conversions are possible though:
        
        >>> Datetime.datetime(dt, fmt='%Y-%m-%dT%H:%M:%S')
            '2014-06-19T17:58:05'
        >>> Datetime.datetime(dt_dict, fmt='%Y-%m-%dT%H:%M:%S')
            '2014-06-19T17:58:05'
        >>> Datetime.datetime(dt_dict, fmt=%Y-%m-%d %H:%M:%S+00')
            '2014-06-19 17:58:05+00'
        >>> Datetime.datetime(dt, fmt='%Y-%m-%d %H:%M:%S')
            '2014-06-19 17:58:05'  
        >>> Datetime.datetime(dt_dict, fmt='%Y-%m-%d')
            '2014-06-19'
        >>> Datetime.datetime(dt, fmt=True) # use ctime()
            "Thu Jun 19 17:58:05 2014"
        >>> Datetime.datetime(dt_dict, fmt='calendar')
            (2014, 25, 4)
        >>> Datetime.datetime(dt_dict, fmt='iso')
            '2014-06-19T17:58:05' 
        >>> Datetime.datetime(dt, fmt='iso')
            '2014-06-19T17:58:05' 
        >>> Datetime.datetime("Thu Jun 19 17:58:05 2014")
            {{'second': 5, 'hour': 17, 'year': 2014, 'day': 19, 'minute': 58, 'month': 6}}
            
        Mind the UTC:
        
        >>> import dateutil
        >>> utc_tz = dateutil.tz.tzutc() # note: same as timing.UTC_TZ
        >>> dt_utc = dt.replace(tzinfo=utc_tz)
        >>> Datetime.datetime(dt_utc, fmt=timestamp')
            1403200685 
        
        As desired, the operation is idempotent (when the right parameters are passed!):        
        
        >>> f_dt = Datetime.datetime(dt, fmt='%Y-%m-%d %H:%M:%S')
        >>> dt == Datetime.datetime(f_dt, fmt='datetime')
            True
        >>> f_dt_utc = Datetime.datetime(dt_utc, fmt='timestamp')
        >>> dt_utc == Datetime.datetime(f_dt_utc, fmt='datetime')
            True
        
        As for the `no_micro_secs` keyword argument, it is useful to avoid some
        'unpleasant' notation:
        
        >>> dt = datetime.datetime.now()
        >>> dt
            datetime.datetime(2014, 8, 18, 14, 56, 17, 821000)
        >>> print dt
            2014-08-18 14:56:17.821000
        >>> Datetime.datetime(dt, fmt='iso')
            '2014-08-18T14:56:17'
        >>> Datetime.datetime(dt, fmt='iso', no_micro_secs=False)
            '2014-08-18T14:56:17.821000'  
            
        Though it does not affect most representations:
        
        >>> Datetime.datetime(dt, fmt=True, no_micro_secs=False)
        'Mon Aug 18 14:56:17 2014'
        """
        no_micro_second = kwargs.pop('no_micro_secs',True)
        dtime, unix, isoformat, dict_, isocal = False, False, False, False, False
        fmt = kwargs.pop('fmt',False)
        if arg in ((),None):            arg = kwargs 
        else:                           arg = arg[0]
        if arg is None:    
            return None
        elif isinstance(arg,string_types) and arg in cls.__DICT_NOW.keys():
            arg = cls.__DICT_NOW[arg]()
            if fmt is False:    dtime = True
        elif isinstance(arg, (int,float)):
            try:    arg = datetime.datetime.fromtimestamp(arg, cls.__DEF_TIMEZ) # we put a tz
            except: raise IOError("Timestamp %s not recognised" % arg) 
        elif not isinstance(arg, (string_types,Mapping,datetime.datetime,datetime.date)):
            raise TypeError("Wrong input date time format") 
        if fmt=='datetime':             fmt, dtime = None, True
        elif fmt=='timestamp':          fmt, unix = None, True
        elif fmt=='iso':                fmt, isoformat = None, True
        elif fmt=='dict':               fmt, dict_ = None, True
        elif fmt=='calendar':           fmt, isocal = None, True
        elif fmt=='default':            fmt = '%Y-%m-%dT%H:%M:%S' #ISO 8601 same as the one returned by datetime.datetime.isoformat 
        elif not isinstance(fmt,(bool,string_types)):  
            raise TypeError("Wrong timing format")
        # special case: already an instance datetime.datetime
        # proceed...
        d_datetime, _datetime = {}, None
        if isinstance(arg,(datetime.datetime,datetime.date)):
            _datetime, arg = arg, arg.ctime()
        # update the possible output formats       
        if isinstance(arg,(string_types,datetime.datetime,datetime.date)) and not (fmt or isoformat or isocal or dict_ or unix):
            dict_ = True
        if isinstance(arg,string_types):
            try:
                if _datetime is None:   _datetime = dateutil.parser.parse(arg)
                [d_datetime.update({unit: getattr(_datetime,unit)}) for unit in cls.DATETIME_KWARGS.values()]
            except:
                for unit in cls.__DATETIME_ITEMS:
                    pattern = r'(.*)\d*\.*\d*\s*' + unit + r'(\d|\s|$)+(.*)' # raw string pattern
                    x = re.search(pattern,arg)                
                    if x is None:       continue
                    elif unit in cls.DATETIME_KWARGS.keys():
                        unit = cls.DATETIME_KWARGS[unit]
                    if d_datetime.get(unit) is None:    d_datetime[unit] = 0
                    d_datetime[unit] += eval(re.match('.*?([.0-9]+\s*)$',x.group(1)).group(1))
        elif isinstance(arg,Mapping):
            for unit in cls.__DATETIME_ITEMS:
                if unit not in arg:         continue
                else:                       t = arg.get(unit)
                if unit in cls.DATETIME_KWARGS.keys():
                    unit = cls.DATETIME_KWARGS[unit]
                if d_datetime.get(unit) is None:    d_datetime[unit] = 0
                d_datetime[unit] += t
        try:                assert _datetime
        except:             _datetime = datetime.datetime(**d_datetime)
        try:                _datetime = _datetime.replace(microsecond=0) if no_micro_second else _datetime
        except:             pass
        if not(fmt or dtime or isoformat or isocal or dict_ or unix):
            dtime = True
        if isoformat:            
            try:    return _datetime.isoformat()
            except: raise IOError("Isoformat not implemented") 
        elif isocal:
            try:    return _datetime.isocalendar()
            except: raise IOError("Isocalendar format not implemented") 
        elif fmt:
            if not any([re.search(s,fmt) for s in ('%Y','%m','%d','%H','%M','%S')]):
                raise IOError("String format is not a standard datetime format")
            try:    return _datetime.ctime() if fmt is True else _datetime.strftime(fmt) 
            except: raise IOError("Format not implemented") 
        elif unix:
            if _datetime.tzinfo is None:
                # _datetime is most likely a naive datetime; we assume it is in fact 
                # a default ('local' or 'utc', depending on __DEF_TIMEZ variable) time            
                _datetime = _datetime.replace(tzinfo=cls.__DEF_TIMEZ)
            try:    
                _datetime = _datetime.astimezone(tz=cls.DICT_TIMEZ['utc'])
            except: pass
             # calendar.timegm() assumes it's in UTC
            try:    return calendar.timegm(_datetime.timetuple())
            except: raise IOError("Unix format not implemented") 
            # instead, time.mktime() assumes that the passed tuple is in local time
            # return time.mktime(_datetime.timetuple())
        elif dtime:      
            return _datetime
        elif dict_:
            return d_datetime
                                              
    #/************************************************************************/
    @classmethod
    def timedelta(cls, *span, **kwargs): 
        """Perform some duration calculations and conversions.
            
            >>> dt = Datetime.timedelta(*span, **kwargs)
            >>> 
                        
        Arguments
        ---------        
        span : datetime.timedelta, str, float, dict
            an object specifying a duration, e.g. it can be any form likewise:                
            
            - a :class:`datetime.timedelta` object: 
              :literal:`datetime.timedelta(2014, 6, 19, 17, 58, 5)`,
            - an explicit duration string: 
              :literal:`'2900mn 45d 500sec 34hr 4m 2y'`,
              (where order does not matter),
            - an equivalent dictionary-like date: 
              :literal:`{{'{years}':2, '{months}':4, '{days}':45, '{hours}':34, '{minutes}':2900, '{seconds}':500}}`.               
                
            When the date is expressed as an 'explicit string' or a dictionary, a time duration 
            unit can be any string in :literal:`['years', 'months', 'days', 'hours', 'minutes', 'seconds']` .
            
            'zero' and 'hour' are also accepted  instead of an explicit duration, and used 
            to represent both null and 1-hour durations.
               
        Keyword Arguments
        -----------------   
        fmt : str
            variable specifying the desired output duration format: it can be any string
            among:            
            
            - 'timedelta' to output a :class:`datetime` object,
            - 'dict' to output a dictionary indexed by the timing units, where a key   
              is any string in :literal:`['years', 'months', 'days', 'hours', 'minutes', 'seconds']`,
            - 'str' to output an explicit duration string (not used).
            
        timing : str
            if instead, some conversions is expected, this is used to pass the desired
            output format: it can be any string in :literal:`['y', 'm', 'd', 'hr', 'mn', 'sec']` .

        Example
        -------
        >>> import datetime
        >>> Datetime.timedelta('hour')
            '3600.0sec'
        
        Let's construct some timedelta objects that are equivalent:
        
        >>> td = datetime.timedelta(900, 57738, 80000)
        >>> td_str = '45d 34hr 2900mn 4m 500sec 2y'
        >>> td_dict = {{'y':2, 'm':4, 'd':45, 'hr': 34, 'mn': 2900, 'sec': 500}}

        as to check the consistency:
        
        >>> Datetime.timedelta(td_str, fmt='dict') == td_dict
            True
        >>> td == Datetime.timedelta(td_str, **{{'fmt': 'timedelta'}}),
            True
        >>> Datetime.timedelta(td_str, dict=True)
            {{'d': 45, 'hr': 34, 'mn': 2900, 'm': 4, 'sec': 500, 'y': 2}}
        >>> Datetime.timedelta(td_dict) == Datetime.timedelta(td_dict_bis)
            True
        >>> Datetime.timedelta(td_str) == Datetime.timedelta('4m 34hr 500sec 2y 2900mn 45d ')
            True
        >>> Datetime.timedelta(td_str, **{{'timing': True}}) == td_dict
            True
        >>> Datetime.timedelta(td_dict, timing=True) == td_str
        
        Note that these one work too:
        
        >>> Datetime.timedelta(**td_dict) == td_str
            True
        >>> Datetime.convert_time_units('d', **td_dict) > td.days
            True
        """
        timing = kwargs.pop('timing',None)
        tdelta, str_, dict_ = False, False, False
        fmt = kwargs.pop('fmt',False)
        if fmt=='timedelta':            fmt, tdelta = None, True
        elif fmt=='dict':               fmt, dict_ = None, True
        elif fmt=='str':                fmt, str_ = None, True#analysis:ignore
        elif not isinstance(fmt,bool):  
            raise   TypeError("Wrong timing format")
        if span in ((),None):           span = kwargs 
        else:                           span = span[0]
        if span is None:    
            return None
        elif isinstance(span, string_types) and span in cls.__DICT_DURATION.keys():
            span = cls.__DICT_DURATION[span]
        elif not isinstance(span, (string_types, Mapping,datetime.timedelta)):
            raise TypeError("Wrong input timing format") 
        # special case: already an instance datetime.timedelta
        if isinstance(span,datetime.timedelta):
            span = {'sec': span.total_seconds()}
        # by default, when no argument is passed, none of the timing/tdelta arguments
        # is passed, we assume  that the return type is not the input one
        dict_ = dict_ or isinstance(span,str)
        # proceed...
        d_span = {}
        if isinstance(span, string_types):
            for unit in cls.__TIMEDELTA_ITEMS:
                pattern = r'(.*)\d*\.*\d*\s*' + unit + r'(\d|\s|$)+(.*)' # raw string pattern
                x = re.search(pattern,span)                
                if x is None:       continue
                elif unit in cls.TIMEDELTA_KWARGS.values():
                    unit = cls.__TIMEDELTA_KWARGS_REV[unit]
                if d_span.get(unit) is None:    d_span[unit] = 0
                d_span[unit] += eval(re.match('.*?([.0-9]+\s*)$',x.group(1)).group(1))
        elif isinstance(span, Mapping):
            for unit in cls.__TIMEDELTA_ITEMS:
                if unit not in span:    continue
                else:                       val_span = span.get(unit)
                if unit in cls.TIMEDELTA_KWARGS.values():
                    unit = cls.__TIMEDELTA_KWARGS_REV[unit]
                if d_span.get(unit) is None:    d_span[unit] = 0
                d_span[unit] += val_span
            span = ' '.join([str(v)+k for (k,v) in d_span.items()]) 
        timing = timing if not tdelta else 'mn'
        if timing in cls.TIMING_UNITS:      
            # convert into one single unit
            span = cls.convert_time_units(timing, **d_span)
            if tdelta:      
                return datetime.timedelta(**{'minutes': span})
            else:           
                return span               
        elif dict_:
            return d_span
        else: #if str_:
            return span

    #/************************************************************************/
    @classmethod
    def span(cls, **kwargs):
        """Calculate the timing span (duration) in between two given beginning 
        and ending date(time)s.
        
            >>> d = Datetime.span(**kwargs)
            
        Keyword Arguments
        -----------------        
        since,until : datetime.datetime, datetime.date, str, float, dict   
            beginning and ending (respectively) time instances whose formats are any 
            of those accepted by :meth:`Datetime.datetime` (not necessarly identical
            for both instances).         
        fmt : str
            additional parameter for formatting the output result: see :meth:`~Datetime.timedelta`;
            default is a :class`datetime.datetime` format.
            
        Returns
        -------
        d : datetime.timedelta, str, float, dict   
            duration between the `since` and `until` timing instances, expressed in any 
            the format passed in `fmt` and accepted by :meth:`Datetime.timedelta`\ .
        
        Example
        -------
        >>> since = datetime.datetime.now()
        >>> print(since) 
            2014-08-18 19:30:40.970000
        >>> until = since + datetime.timedelta(10) # 10 days
        >>> until # this is a datetime
            datetime.datetime(2014, 8, 28, 19, 30, 40, 970000)
        >>> print(until) # which displays microseconds
            2014-08-28 19:30:40.970000
        >>> print(until - since)
            10 days, 0:00:00
        >>> Datetime.span(since=since, until=until)
            datetime.timedelta(10)
        >>> Datetime.span(since=since, until=until, fmt='str')
            '864000.0sec'

        The fact that microseconds are not taken into account ensures the consistency
        of the results when converting in between different formats:             
        
        >>> until_iso = Datetime.datetime(until, fmt='iso')
        >>> until_iso
            '2014-08-28T19:30:40'
        >>> print(until_iso) # microseconds have been dumped...
            2014-08-28T19:30:40
        >>> Datetime.span(since=since, until=until_iso)
            datetime.timedelta(10) # 10 days as set
            
        However, if we were precisely taking into account the microseconds:
        
        >>> Datetime.span(since=since, until=until_iso, no_micro_secs=False)
            datetime.timedelta(9, 86399, 30000)

        while this obviously does not affect the precise calculation:
        
        >>> Datetime.span(since=since, until=until, no_micro_secs=False)
            datetime.timedelta(10)
        """
        since, until = kwargs.pop('since',None), kwargs.pop('until',None)       
        if not(since and until):
            raise IOError("Missing arguments")          
        no_micro_second = kwargs.pop('no_micro_secs',True)
        kw = {'fmt': 'datetime', 'no_micro_secs': no_micro_second}
        since, until = cls.datetime(since, **kw), cls.datetime(until, **kw)
        if operator.xor(until.tzinfo is None, since.tzinfo is None):
            if until.tzinfo is None:    until = until.replace(tzinfo=cls.__DEF_TIMEZ)
            else:                       since = since.replace(tzinfo=cls.__DEF_TIMEZ)
        span = until - since
        if not kwargs.get('fmt') or kwargs.get('fmt')=='datetime':   
            return span
        else:
            return cls.timedelta(span, **kwargs) 
        
    #/************************************************************************/
    @classmethod
    def since(cls, **kwargs):
        """Calculate a beginning date(time) given a duration and an ending date(time).
        
            >>> s = Datetime.since(**kwargs)) 
                       
        Keyword Arguments
        -----------------        
        until : datetime.datetime, datetime.date, str, float, dict   
            ending date instance whose format is any of those accepted by :meth:`Datetime.datetime`.         
        span : datetime.timedelta, str, float, dict   
            duration expressed in any of the formats accepted by :meth:`Datetime.timedelta`\ .
        fmt : str
            additional parameter for formatting the output result: see :meth:`Datetime.datetime`\ .
            
        Returns
        -------
        s : datetime.datetime, str, float, dict   
            beginning date estimated from :literal:`until` and :literal:`span` timing 
            arguments, expressed in any format as in :literal:`fmt` and  accepted by 
            :meth:`Datetime.datetime`\ .

        Example
        -------
        >>> since = datetime.datetime.now() # this is: datetime.datetime(2014, 8, 19, 16, 27, 41, 629000)
        >>> print(since)
            2014-08-19 16:27:41.629000        
        >>> span = '1d'
        >>> until = since + datetime.timedelta(1)
        >>> print(until)
            2014-08-20 16:27:41.629000
        >>> print Datetime.since(until=until, span=span)
            {{'second': 41, 'hour': 16, 'year': 2014, 'day': 20, 'minute': 27, 'month': 8}}
        >>> since_c = Datetime.since(until=until, span=span, fmt='datetime')
        >>> print(since_c) # this is: datetime.datetime(2014, 8, 19, 16, 23, 27)
            2014-08-20 16:27:41
        >>> since_c == since
            False
        >>> since_c - since # we missed the microseconds...
            datetime.timedelta(0, 86399, 371000)
            
        Again, we can ensure precision by including the microseconds in the calculation:

        >>> since_c = Datetime.since(until=until, span=span, fmt='datetime', no_micro_secs=False)
        >>> print since_c # this is datetime.datetime(2014, 8, 20, 16, 27, 41, 629000)
            2014-08-19 16:27:41.629000
        >>> since_c == since 
            True

        Other results are as expected, independently of the format of the input arguments:
        
        >>> until_iso = Datetime.datetime(until, fmt='iso')
        >>> print(until_iso)
            2014-08-20T16:27:41
        >>> Datetime.since(until=until_iso, span=span, fmt=dict)
            {{'second': 41, 'hour': 16, 'year': 2014, 'day': 19, 'minute': 27, 'month': 8}}
        """
        until, span = kwargs.pop('until',None), kwargs.pop('span',None) 
        if not(until and span):
            raise IOError("Missing arguments")    
        # make a precise calculation, up to the microsecond
        since = cls.datetime(until, fmt='datetime', no_micro_secs=False)  \
            - cls.timedelta(span, fmt='timedelta')
        # however return as desired
        if (not kwargs.get('fmt') or kwargs.get('fmt')=='datetime')      \
            and not kwargs.get('no_micro_secs',True):   
            return since
        else:
            return cls.datetime(since, **kwargs) 
        
    #/************************************************************************/
    @classmethod
    def until(cls, **kwargs):
        """Calculate a beginning date(time) given a duration and an ending date(time).
        
            >>> u = Datetime.until(**kwargs)) 
                       
        Keyword Arguments
        -----------------        
        since : datetime.datetime, datetime.date, str, float, dict   
            beginning date instance whose format is any of those accepted by :meth:`Datetime.datetime`\ .         
        span,fmt :   
            see :meth:`Datetime.since`.
            
        Returns
        -------
        u : datetime.datetime, str, float, dict   
            ending date estimated from :literal:`since` and :literal:`span`
            timing arguments, expressed in any format as in :literal:`fmt` and 
            accepted by :meth:`Datetime.datetime`\ .
            
        'second': 41, 'hour': 16, 'year': 2014, 'day': 20, 'minute': 27, 'month': 8 

        Example
        -------
        >>> since = datetime.datetime.now() # this is: datetime.datetime(2014, 8, 19, 16, 45, 5, 94000)
        >>> print(since)
            2014-08-19 16:45:05.094000        
        >>> span = datetime.timedelta(2) # 2 days
        >>> print(span)
            2 days, 0:00:00
        >>> until = Datetime.until(since=since, span=span, fmt='iso')
        >>> print(until)
            '2014-08-21T16:45:05'   
        >>> Datetime.since(until=until, span=span, fmt='datetime')
            datetime.datetime(2014, 8, 19, 16, 45, 5)           
        """
        since, span = kwargs.pop('since',None), kwargs.pop('span',None) 
        if not(since and span):
            raise IOError('missing arguments')           
        # make a precise calculation, up to the microsecond
        until = cls.datetime(since, fmt='datetime', no_micro_secs=False)  \
            + cls.timedelta(span, fmt='timedelta')
        # however return as desired
        if (not kwargs.get('fmt') or kwargs.get('fmt')=='datetime')      \
            and not kwargs.get('no_micro_secs',True):   
            return until
        else:
            return cls.datetime(until, **kwargs) 

    #/************************************************************************/
    @classmethod
    def gt(cls, time1, time2):
        """Compare two date/time instances: check that the first entered time instance 
        is posterior (after) to the second one.
        
            >>> resp = Datetime.gt(time1, time2)
            
        Arguments
        ---------        
        time1,time2 : datetime.datetime, datetime.date, str, float, dict       
            time instances whose format are any accepted by :meth:`Datetime.datetime`\ . 
            
        Returns
        -------
        resp : bool
            :literal:`True` if `time1`>`time2`, i.e. `time1` represents a time
            posterior to `time2`; :literal:`False` otherwise. 

        Example
        -------
        >>> dt = datetime.datetime(2014, 6, 19, 17, 58, 5)
        >>> one_day = {{'y':2014, 'm':6, 'd':19, 'hr':17, 'mn':58,  'sec':5}}
        >>> the_day_after = one_day.copy()
        >>> the_day_after.update({{'d': the_day_after['d']+1}})
        >>> Datetime.gt(the_day_after, one_day)
            True
            
        See also
        -------- 
        :meth:`~Datetime.lt`, :meth:`~Datetime.gte`
        """
        t1 = Datetime.datetime(time1, fmt='datetime')
        t2 = Datetime.datetime(time2, fmt='datetime')
        if t1.tzinfo is None:    t1 = t1.replace(tzinfo=cls.__DEF_TIMEZ)
        if t2.tzinfo is None:    t2 = t2.replace(tzinfo=cls.__DEF_TIMEZ)
        try:    return t1 - t2 > cls.ZERO
        except: raise IOError("Unrecognised time operation")

    #/************************************************************************/
    @classmethod
    def lt(cls, time1, time2):
        """Compare two date/time instances: check that the first entered time instance 
        is prior (before) to the second one.
        
            >>> resp = Datetime.lt(time1, time2)
            
        Arguments
        ---------        
        time1,time2 : datetime.datetime, str, float, dict       
            see :meth:`~Datetime.gt`\ .
            
        Returns
        -------
        resp : bool
            :literal:`True` if `time1`<`time2`, i.e. `time1` represents a time
            prior to `time2`; :literal:`False` otherwise. 
            
        Example
        -------
        Following :meth:`~Datetime.gt` example:
        
        >>> Datetime.lt(one_day,the_day_after)
            True
        >>> one_day_iso = Datetime.datetime(one_day, **{{'{KW_FORMAT_DATETIME}': '{KW_ISOFORMAT}'}})
        >>> one_day_iso
            '2014-06-19T17:58:05'
        >>> Datetime.lt(one_day_iso,the_day_after)
            True
            
        See also
        -------- 
        :meth:`~Datetime.gt`, :meth:`~Datetime.lte`
        """
        return cls.gt(time2, time1)

    #/************************************************************************/
    @classmethod
    def gte(cls, time1, time2):
        """Compare two date/time instances: check that the first entered time instance 
        is posterior to  or simultaneous with the second one the second one.
        
            >>> resp = Datetime.gte(time1, time2)
        
        Example
        -------
        .. Following :meth:`~Datetime.lt` and :meth:`~Datetime.gt` examples:
        
        >>> Datetime.gte(one_day_iso,one_day) and not Datetime.gt(one_day_iso,one_day)
            True
            
        See also
        -------- 
        :meth:`~Datetime.gt`, :meth:`~Datetime.lte`
        """
        # return cls.greater(time1, time2) or time1==time2
        t1 = Datetime.datetime(time1, fmt='datetime')
        t2 = Datetime.datetime(time2, fmt='datetime')
        if not t1.tzinfo:    t1 = t1.replace(tzinfo=cls.__DEF_TIMEZ)
        if not t2.tzinfo:    t2 = t2.replace(tzinfo=cls.__DEF_TIMEZ)
        try:    return t1 - t2 >= cls.ZERO
        except: raise IOError("Unrecognised time operation")

    #/************************************************************************/
    @classmethod
    def lte(cls, time1, time2):
        """Compare two date/time instances: check that the first entered time instance 
        is prior to or simultaneous with the second one. 
        
            >>> resp = Datetime.lte(time1, time2)
        
        Example
        -------
        .. Following :meth:`~Datetime.lt` :meth:`~Datetime.gt` examples:
        
        >>> Datetime.lte(one_day_iso,one_day) and not Datetime.lt(one_day_iso,one_day)
            True
            
        See also
        -------- 
        :meth:`~Datetime.lt`, :meth:`~Datetime.gte`
        """
        return cls.gte(time2, time1)

    #/************************************************************************/
    @staticmethod
    def dtformat(**kwargs):
        """Determine a datetime format from date and time formats, and their combination. 
        
            >>> dt = dtformat(**kwargs)
            
        Keyword Arguments
        -----------------
        date,time : str
            strings specifying the date and time (written) formats; default is :literal:`'%Y-%m-%d'` 
            for :data:`date` and :literal:`'%H:%M:%S'` for :data:`time`.
        fmt : str
            string specifying how :data:`date` and :data:`time` format are combined;
            default is: :literal:`'%sT%s'`, i.e. the char :literal:`'T'` separates date and time.
            
        Returns
        -------
        dt : str
            string defining the format of datetime as :literal:`fmt.format(date,time)`;
            default to ISO 8601 format: :literal:`'%Y-%m-%dT%H:%M:%S'`\ .
        """
        __dateformat = '%Y-%m-%d'               #ISO 8601
        dateformat = kwargs.pop('date',__dateformat) 
        __timeformat = '%H:%M:%S'               #ISO 8601
        timeformat = kwargs.pop('time',__timeformat) 
        __datetimeformat = '%sT%s'  #('%Y%m%d%H%M%S')
        datetimeformat = kwargs.pop('fmt',__datetimeformat)
        return datetimeformat % (dateformat,timeformat)
    
    #/************************************************************************/
    @staticmethod
    def timestamp(**kwargs):
        """Return a timestamp.
        
            >>> dtnow = timestamp(**kwargs)
            
        Keyword Arguments
        -----------------
        date,time,fmt : str
            see :meth:`datetimeformat`\ .
            
        Returns
        -------
        dtnow : str
            timestamp estimated as the datetime representation of `now` (i.e. at the
            time the method is called).
        """
        return Datetime.NOW().strftime(Datetime.dtformat(**kwargs))

 
#==============================================================================
#%% Class File
    
class File(object):
    """Static methods for Input/Output file processing.
    """
    
    #/************************************************************************/
    @staticmethod
    def file_exists(filepath, return_path=False): 
        """A case insensitive file existence checker.
    
            >>> a, p = File.file_exists(file, return_path)
            
        Arguments
        ---------
        file : str
            file whose existence is to be checked.
        return_path : bool, optional 
            flag set to return the case sensitive path; def.:  return_path=False.
    
        Returns
        -------
        a : bool
            `True`/`False` answer to the existence.
        p : str
            optional full path to the case sensitive path.
        """
        def file_check(filepath):
            if not osp.exists(filepath):
                raise IOError("Path not found: '%s'" % filepath)
            return filepath.strip()  
        if ISWIN: # Windows is case insensitive anyways
            if return_path:         return osp.exists(filepath),filepath 
            else:                   return osp.exists(filepath)
        path, name = osp.split(osp.abspath(filepath))
        files = os.listdir(path)
        for f in files:
            if re.search(f, name, re.I):
                if return_path:     return True, osp.join(path,f)
                else:               return True                
        if return_path:             return False, None
        else:                       return False

    #/************************************************************************/
    @staticmethod
    def find_file(directory, pattern):
        """Find files in a directory and all subdirectories that match a given pattern.
        
            >>> lst_files = File.find_file(directory, pattern)
        """
        # for root, dirs, files in os.walk(directory):
        #     for basename in files:
        #         if fnmatch.fnmatch(basename, pattern):
        #             filename = osp.join(root, basename)
        #             yield filename
        from fnmatch import filter
        results = []
        for root, dirs, files in os.walk(directory):
            #print files
            #print fnmatch.filter(files, pattern)
            results.extend(osp.join(root, f) for f in filter(files, pattern))
        return results

    #/************************************************************************/
    @staticmethod
    def make_dir(directory):
        """Safe directory creation (mkdir) command.
        
            >>> File.make_dir(directory)
        """
        if not osp.exists(directory): os.mkdir(directory)
        return directory

    #/************************************************************************/
    @staticmethod
    def remove(filepath):
        """Safe remove (rm) command.
        
            >>> File.remove(filepath)
        """
        if osp.exists(filepath):    os.remove(filepath)

    #/************************************************************************/
    @staticmethod
    def fill_path(rootpath, relfolderpath, relfilepath):
        """Determine the absolute path of a file given by its relative path wrt 
        a folder itself given by a relative path wrt root path.
        
            >>> path = File.fill_path(rootpath, relfolderpath, relfilepath)
        """
        folderpath = osp.join(rootpath, relfolderpath)
        return osp.abspath(osp.join(folderpath, relfilepath))

    #/************************************************************************/
    @staticmethod
    def uuid(filepath):
        """Generate a uuid reproducible based on filename.
        
            >>> id = File.uuid(filepath)
        """
        from uuid import uuid3, NAMESPACE_DNS
        filepath = File.normcase(File.realpath(filepath))
        return str(uuid3(NAMESPACE_DNS,filepath))

    #/************************************************************************/
    @staticmethod
    def file_info(filepath, **kwargs):
        """Provide various information about a file.
        
            >>> info = File.file_info(filepath, **kwargs)
            
        Keyword Arguments
        -----------------
        date,time,fmt : str
            see :meth:`datetimeformat`\ .
        
        Returns
        -------
        info : dict
            dictionary containing various information about :data:`filepath`, namely
            its :data:`uuid`, :data:`size`, date of modification (:data:`datemodified`), 
            date of creation (:data:`datecreated`), date of last access (:data:`dateaccessed`), 
            the owner's id (:data:`ownerid`) and the owner's name (:data:`ownername`).
        """
        if not osp.exists(filepath):    
            raise IOError("File '%s' not found" % filepath)
        fileinfo = {
            'size':0,
            'datemodified':'',
            'datecreated': '',
            'dateaccessed':''
        }    
        def _winFileOwner(filepath):
            import win32com.client
            import win32net, win32netcon        
            OWNERID=8
            try:
                d=osp.split(filepath)
                oShell = win32com.client.Dispatch("Shell.Application")
                oFolder = oShell.NameSpace(d[0])
                ownerid=str(oFolder.GetDetailsOf(oFolder.parsename(d[1]), OWNERID))
                ownerid=ownerid.split('\\')[-1]
            except: ownerid='0'
            try:
               dc=win32net.NetServerEnum(None,100,win32netcon.SV_TYPE_DOMAIN_CTRL)
               if dc[0]:
                   dcname=dc[0][0]['name']
                   ownername=win32net.NetUserGetInfo(r"\\"+dcname,ownerid,2)['full_name']
               else:
                   ownername=win32net.NetUserGetInfo(None,ownerid,2)['full_name']
            except: ownername='No user match'
        
            return ownerid,ownername
        def _ixFileOwner(uid): # Posix, Unix, ...
            import pwd
            pwuid = pwd.getpwuid(uid)
            ownerid = pwuid[0]
            ownername = pwuid[4]
            return ownerid,ownername
        try:
            filepath = File.normcase(osp.realpath(filepath))
            filestat = os.stat(filepath)
    
            fileinfo['filename'] = osp.basename(filepath)
            fileinfo['filepath'] = filepath
            fileinfo['size'] = filestat.st_size
            fileinfo['datemodified'] = \
                time.strftime(Datetime.dtformat(**kwargs), time.localtime(filestat.st_mtime))
            fileinfo['datecreated'] = \
                time.strftime(Datetime.dtformat(**kwargs), time.localtime(filestat.st_ctime))
            fileinfo['dateaccessed'] = \
                time.strftime(Datetime.dtformat(**kwargs), time.localtime(filestat.st_atime))
            fileinfo['uuid'] = File.uuid(filepath)
            #if sys.platform[0:3].lower()=='win':
            if ISWIN:       ownerid, ownername = _winFileOwner(filepath)
            else:           ownerid, ownername = _ixFileOwner(filestat.st_uid)
            fileinfo['ownerid'] = ownerid
            fileinfo['ownername'] = ownername
        finally:
            return fileinfo

    #/************************************************************************/
    @staticmethod
    def cache():
        platform = sys.platform
        if platform.startswith("win"): # windows
            basedir = os.getenv("LOCALAPPDATA",os.getenv("APPDATA",osp.expanduser("~")))
        elif platform.startswith("darwin"): # Mac OS
            basedir = osp.expanduser("~/Library/Caches")
        else:
            basedir = os.getenv("XDG_CACHE_HOME",osp.expanduser("~/.cache"))
        return osp.join(basedir, PACKNAME)    

    #/****************************************************************************/
    def writable(filepath):
        """Determine if a temporary file can be written in the same directory as a 
        given file.
        
            >>> resp = File.writable(filepath)
        """
        if not osp.isdir(filepath):
            filepath = osp.dirname(filepath)
        try:
            from tempfile import TemporaryFile
            tmp = TemporaryFile(dir=filepath) # can we write a temp file there...?
        except: return False
        else:
            del tmp
            return True

    #/************************************************************************/
    @staticmethod
    def normcase(filepath):
        """Normalize case of pathname by making all characters lowercase and all slashes
        into backslashes.
        
            >>> new = File.normcase(filepath)
        """
        #if type(filepath) in [list,tuple]:
        if not hasattr(filepath,'__iter__'):    return osp.normcase(filepath)
        else:           return [osp.normcase(i) for i in filepath] # iterable
    
    #/************************************************************************/
    @staticmethod
    def basename(filepath):
        """Extract the base name of a file.
        
            >>> base = basename(filepath)
        """
        # return osp.splitext(osp.split(filepath)[1])[0]
        return osp.splitext(osp.basename(filepath))[0]
    
    #/****************************************************************************/
    def extname(filepath, newext):
        """Change the extension of a file.
        
            >>> ext = extname(filepath)
        """
        if not newext.startswith('.'): newext = '.' + newext
        base = osp.splitext(filepath)[0]
        return base + newext
    
    #/************************************************************************/
    @staticmethod
    def filename(filepath):
        """Retrieve the...file name.
        
            >>> name = filename(filepath)
        """
        return osp.split(filepath)[1]
        
    #/************************************************************************/
    @staticmethod
    def normpath(filepath):
        """Normalize path, eliminating double slashes, etc.
        
            >>> new = normpath(filepath)
        """
        if not hasattr(filepath,'__iter__'):    
            return osp.normpath(filepath)
        else:                      
            return [osp.normpath(i) for i in filepath]
        
    #/************************************************************************/
    @staticmethod
    def realpath(filepath):
        """Return the absolute version of a path.
        
            >>> real = realpath(filepath)
    
        Note
        ----
        `osp.realpath/os.path.abspath` returns unexpected results on windows if `filepath[-1]==':'`        
        """
        if hasattr(filepath,'__iter__'): # is iterable
            if ISWIN: # global variable
                realpath=[]
                for f in filepath:
                    if f[-1]==':':f+='\\'
                    realpath.append(osp.realpath(f))
            else:
                return [osp.realpath(f) for f in filepath]
        else:
            if ISWIN and filepath[-1]==':':     filepath+='\\'
            return osp.realpath(filepath)
    
    #/************************************************************************/
    @staticmethod
    def unzip(file, **kwargs):
        try:
            assert zipfile.is_zipfile(file)
        except:
            raise TypeError("Zip file '%s' not recognised" % file)
        cache = kwargs.pop('cache', File.cache())
        operators = [op for op in ['open', 'extract', 'extractall', 'getinfo', 'namelist', 'read', 'infolist'] \
                     if op in kwargs.keys()] 
        try:
            assert operators in ([],[None]) or sum([1 for op in operators]) == 1
        except:
            raise IOError("Only one operation supported per call")
        else:
            if operators in ([],[None]):
                operator = 'extractall'
                kwargs.update({operator: cache})
            else:
                operator = operators[0] 
        members, path = None, None
        if operator in ('open', 'extract', 'getinfo', 'read'):
            members = kwargs.pop(operator, None)
        elif operator == 'extractall':
            path = kwargs.pop('extractall', None)
        else: # elif operator in ('infolist','namelist'):
            try:
                assert kwargs.get(operator) not in (False,None)
            except:
                raise IOError("No operation parsed")
        if operator.startswith('extract'):
            warnings.warn("\n! Data extracted from zip file will be physically stored on local disk !")
        if isinstance(members,string_types):
            members = [members,]
        with zipfile.ZipFile(file) as zf:
            namelist, infolist = zf.namelist(), zf.infolist() 
            _namelist = [osp.basename(n) for n in namelist]
            #if operator in  ('infolist','namelist'):
            #        return getattr(zf, operator)()
            if operator == 'namelist':
                return namelist if len(namelist)>1 else namelist[0]
            elif operator == 'infolist':
                return infolist if len(infolist)>1 else infolist[0]
            elif operator == 'extractall':                
                return zf.extractall(path=path)   
            if members is None and len(namelist)==1:
                members = namelist
            elif members is not None:
                for i in reversed(range(len(members))):
                    m = members[i]
                    try:
                        assert m in namelist
                    except:
                        try:
                            assert m in _namelist
                        except:
                            warnings.warn("\n! File '%s' not found in source zip !" % m)
                            members.pop(i)
                        else:
                            members[i] = namelist[_namelist.index(m)]
            # now: operator in ('extract', 'getinfo', 'read')
            if members in ([],None):
                raise IOError("Impossible to retrieve member file(s) from zipped data")
            results = {m: getattr(zf, operator)(m) for m in members}
        return results
        # raise IOError("Operation '%s' failed" % operator)
