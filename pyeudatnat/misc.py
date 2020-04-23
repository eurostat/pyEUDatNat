#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.. _misc

Module implementing miscellaneous useful methods.

**Dependencies**

*require*:      :mod:`os`, :mod:`six`, :mod:`collections`, :mod:`inspect`, :mod:`re`, 
                :mod:`numpy`, :mod:`time`

*optional*:     :mod:`datetime`, :mod:`zipfile`

*call*:         :mod:`pyeudatnat`         

**Contents**
"""

# *credits*:      `gjacopo <gjacopo@ec.europa.eu>`_ 
# *since*:        Sun Apr 19 16:36:19 2020


import io, os, sys#analysis:ignore
from os import path as osp
import inspect
import re
import warnings#analysis:ignore

from collections import OrderedDict, Mapping, Sequence#analysis:ignore
from six import string_types

import time
try: 
    from datetime import datetime
except ImportError:            
    pass 

import numpy as np

try:
    import zipfile
except:
    _is_zipfile_installed = False
else:
    _is_zipfile_installed = True

from pyeudatnat import PACKNAME

ISWIN           = os.name=='nt' # sys.platform[0:3].lower()=='win'


#%%

#==============================================================================
# Class Miscellaneous
#==============================================================================

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


#%%
#==============================================================================
# Class Type
#==============================================================================

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

    # Python pack types <-> Numpy conversion
    ppt2npt = lambda t: Type.__PPT2NPT[t]
    # note regarding np.dtype:
    #   np.dtype('B')           -> dtype('uint8')
    #   np.dtype('uint8')       -> dtype('uint8')
    #   np.dtype(np.uint8)      -> dtype('uint8')
    # so that on the current machine where it is implemented
    # assert ppt2npy == {'B':'uint8','b': 'int8','H':'uint16','h':'int16','I':'uint32',
    #   'i':'int32', 'f':'float32', 'd':'float64'}
    # but... in the future?!         
    
    # Numpy -> Python pack types conversion
    npt2ppt = lambda t: dict(Type.__PPT2NPT.values(), Type.__PPT2NPT.keys())[t]
    
    # Dictionary of Python -> Numpy
    __NPT2PYT = { np.dtype('b'):                bool,
                  np.dtype('i'):                bool,
                  np.dtype('O'):                str,
                  object:                       str,
                  np.dtype('i'):                int, 
                  np.dtype('uint32'):           int, 
                  np.dtype('int'):              int, 
                  np.dtype('f'):                float, 
                  np.dtype(float):              float,
                  np.dtype('datetime64'):       datetime,
                  np.dtype('datetime64[ns]'):   datetime
                 } 

    # Numpy -> Python types conversion
    npt2pyt = lambda t: Type.__NPT2PYT[t]
    
    # Pandas -> Python types conversion
    pdt2pyt = npt2pyt
  
    # Dictionary of Python -> Numpy
    def __rev_dict_unique_values(d):
        dd = {}
        [dd.setdefault(v, []).append(k) for (k,v) in d.items()]  
        return dd 
    __PYT2NPT = __rev_dict_unique_values(__NPT2PYT)        
    #__PY2NPT = { bool:      [np.dtype('b'), np.dtype('i')],
    #             str:        [np.dtype('O'), object], 
    #             int:        [np.dtype('i'), np.dtype('uint32'), np.dtype('int')],
    #             float:      [np.dtype('f'), np.dtype(float)],
    #             datetime:   [np.dtype('datetime64'), np.dtype('datetime64[ns]')],
    #             } 
        
    # Python -> Numpy types conversion
    pyt2npt = lambda t: Type.__PYT2NPT[t]
    
    # Python -> Pandas types conversion
    pyt2pdt = pyt2npt
    
    # Python type name -> Numpy type conversion
    pytname2npt = lambda t: {k.__name__:v for (k,v) in Type.__PYT2NPT.items()}[t]

    # Python type name -> Pandas type conversion
    pytname2pdt = pytname2npt
    

#%%
#==============================================================================
# Class Datetime
#==============================================================================
    
class Datetime(object):
    """Static methods for datetime objects manipulation.
    """

    #/************************************************************************/
    @staticmethod
    def datetime_format(**kwargs):
        """Determine a datetime format from date and time formats, and their combination. 
        
            >>> dt = datetime_format(**kwargs)
            
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
        return datetime.now().strftime(Datetime.datetime_format(**kwargs))
    
 
#%%
#==============================================================================
# Class File
#==============================================================================
    
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
                time.strftime(Datetime.datetime_format(**kwargs), time.localtime(filestat.st_mtime))
            fileinfo['datecreated'] = \
                time.strftime(Datetime.datetime_format(**kwargs), time.localtime(filestat.st_ctime))
            fileinfo['dateaccessed'] = \
                time.strftime(Datetime.datetime_format(**kwargs), time.localtime(filestat.st_atime))
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
            data = [getattr(zf, operator)(m) for m in members]
        return (data, members) if data in ([],[None]) or len(data)>1 \
            else (data[0], members)
        # raise IOError("Operation '%s' failed" % operator)
