#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.. _text

.. Links

.. _googletrans: https://github.com/ssut/py-googletrans
.. |googletrans| replace:: `googletrans <googletrans_>`_

Module implementing miscenalleous text processing methods, including translation.

**Dependencies**

*require*:      :mod:`os`, :mod:`six`, :mod:`collections`, :mod:`numpy`, :mod:`pandas`

*optional*:     :mod:`googletrans`

*call*:         :mod:`pyeudatnat`

**Contents**
"""

# *credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_
# *since*:        Thu Apr  9 09:56:45 2020

#%% Settings

import re
import logging

from collections import OrderedDict
from collections.abc import Mapping, Sequence
from six import string_types

try:
    assert True
    import googletrans as gtrans
except (AssertionError,ImportError):
    # logging.warning('\n! missing googletrans package (https://github.com/ssut/py-googletrans) - Translations not available !')
    _is_googletrans_installed = False
else:
    # logging.warning('\n! googletrans help: https://py-googletrans.readthedocs.io/en/latest !')
    _is_googletrans_installed = True

try:
    assert False # not used: ignore
    import Levenshtein
except (AssertionError,ImportError):
    _is_levenshtein_installed = False
    # logging.warning('\n! missing python-Levenshtein package (http://github.com/ztane/python-Levenshtein) - Text matching not available !')
else:
    # logging.warning('\n! Levenshtein help: https://rawgit.com/ztane/python-Levenshtein/master/docs/Levenshtein.html !')
    _is_levenshtein_installed = True

from pyeudatnat import COUNTRIES#analysis:ignore

LANGS           = { ## alpha-3/ISO 639-2 codes
                    'sq': 'albanian',
                    'ar': 'arabic',
                    'hy': 'armenian',
                    'az': 'azerbaijani',
                    'eu': 'basque',
                    'be': 'belarusian',
                    'bs': 'bosnian',
                    'bg': 'bulgarian',
                    'ca': 'catalan',
                    'co': 'corsican',
                    'hr': 'croatian',
                    'cs': 'czech',
                    'da': 'danish',
                    'nl': 'dutch',
                    'en': 'english',
                    'eo': 'esperanto',
                    'et': 'estonian',
                    'fi': 'finnish',
                    'fr': 'french',
                    'fy': 'frisian',
                    'gl': 'galician',
                    'ka': 'georgian',
                    'de': 'german',
                    'el': 'greek',
                    'hu': 'hungarian',
                    'is': 'icelandic',
                    'ga': 'irish',
                    'it': 'italian',
                    'la': 'latin',
                    'lv': 'latvian',
                    'lt': 'lithuanian',
                    'lb': 'luxembourgish',
                    'mk': 'macedonian',
                    'mt': 'maltese',
                    'no': 'norwegian',
                    'pl': 'polish',
                    'pt': 'portuguese',
                    'ro': 'romanian',
                    'ru': 'russian',
                    'gd': 'scots gaelic',
                    'sr': 'serbian',
                    'sk': 'slovak',
                    'sl': 'slovenian',
                    'es': 'spanish',
                    'sv': 'swedish',
                    'tr': 'turkish',
                    'uk': 'ukrainian',
                    'cy': 'welsh'
                    }
# CODELANGS = dict(map(reversed, LANGS.items())) # {v:k for (k,v) in LANGS.items()}

DEF_LANG        = 'en'


#%% Core functions/classes

#==============================================================================
# Method isoLang
#==============================================================================

def isoLang(arg):
    """Given a language or an ISO 639 locale code, return the pair {language,locale}.

        >>> Text.isoLang(locale_or_language)
    """
    if not (arg is None or isinstance(arg, (string_types,Mapping))):
        raise TypeError("Wrong format for language/locale '%s' - must be a string or a dictionary" % arg)
    elif isinstance(arg, string_types):
        if arg in LANGS.keys():
            language, locale = None, arg
        elif arg in LANGS.values():
            language, locale = arg, None
        else:
            raise IOError("Language/locale '%s' not recognised" % arg)
    elif isinstance(arg, Mapping):
        locale, language = arg.get('code', None), arg.get('name', None)
    else: # lang is None
        language, locale = None, None
    if locale in ('', None) and language in ('', None):
        try:
            lang = {'code': DEF_LANG, 'name': LANGS[DEF_LANG]} # not implemented
        except:     language, locale = None, None
        else:
            locale, language = lang.get('code',None), lang.get('name',None)
    elif locale in ('', None): # and NOT language in ('', None)
        try:
            locale = dict(map(reversed, LANGS.items())).get(language)
        except:     locale = None
    elif language in ('', None): # and NOT locale in ('', None)
        try:
            language = LANGS.get(locale)
        except:     language = None
    return {'code': locale, 'name': language}


#==============================================================================
# Class Interpret
#==============================================================================

class Interpret(object):

    try:
        assert _is_googletrans_installed is True
        UTRANSLATOR = gtrans.Translator() # parameter independent: we use a class variable
    except:     pass
    #else: # see https://github.com/ssut/py-googletrans/issues/257
    #    UTRANSLATOR.raise_Exception = True

    @classmethod
    def detect(cls, *text, **kwargs):
        """Language detection method.

            >>> Interpret.detect(*text, **kwargs)
        """
        try:
            assert _is_googletrans_installed is True
        except:
            raise ImportError("'detect' method not available")
        text = (text not in ((None,),()) and text[0])               or \
                kwargs.pop('text', '')
        if isinstance(text, string_types):
            text = [text,]
        if isinstance(text, Sequence) and all([isinstance(t, string_types) for t in text]):
            pass
        elif text in (None,(),''):
            return
        else:
            raise TypeError("Wrong format for input text '%s'" % text)
        try:
            return [r.lang for r in [cls.UTRANSLATOR.detect(t) for t in text]]
        except:
            return [r['lang'] for r in cls.UTRANSLATOR.detect(text)]

    @classmethod
    def translate(cls, *text, **kwargs):
        """Translation method.

            >>> Interpret.translate(*text, **kwargs)
        """
        try:
            assert _is_googletrans_installed is True
        except:
            raise ImportError("'translate' method not available")
        text = (text not in ((None,),()) and text[0])               or \
                kwargs.pop('text', '')
        if isinstance(text, string_types):
            text = [text,]
        if isinstance(text, Sequence) and all([isinstance(t, string_types) for t in text]):
            pass
        elif text in (None,(),''):
            return
        else:
            raise TypeError("Wrong format for input text '%s'" % text)
        ilang, olang = kwargs.pop('ilang', None), kwargs.pop('olang', DEF_LANG)
        if not (isinstance(ilang, string_types) and isinstance(olang, string_types)):
            raise TypeError("Languages not recognised")
        if 'filt' in kwargs:
            f = kwargs.get('filt')
            try:                    assert callable(f)
            except AssertionError:  pass
            else:
                text = [f(t) for t in text]
        if ilang == olang or text == '':
            return text
        try:
            return [t.text for t in [cls.UTRANSLATOR.translate(_, src=ilang, dest=olang) for _ in text]]
        except:
            return [t.text for t in cls.UTRANSLATOR.translate(text, src=ilang, dest=olang)]


#==============================================================================
# Class TextProcess
#==============================================================================

class TextProcess(object):
    """Text handling and string manipulation.
    """

    #/************************************************************************/
    @staticmethod
    def match_close(t1, t2, dist='jaro_winkler'):
        """Text matching method.
        """
        try:
            assert _is_levenshtein_installed is True
        except:
            raise ImportError("'match_close' method not available")
        try:
            distance = getattr(Levenshtein,dist)
        except AttributeError:
            raise AttributeError("Levenshtein distance '%s' not recognised" % distance)
        else:
            return distance(t1.str.upper().str, t2)

    #/************************************************************************/
    @staticmethod
    def split_at_upper(s, contiguous=True):
        """Text splitting method.

        Description
        -----------
        Split a string at uppercase letters
        """
        strlen = len(s)
        lower_around = (lambda i: s[i-1].islower() or strlen > (i + 1) and s[i + 1].islower())
        start, parts = 0, []
        for i in range(1, strlen):
            if s[i].isupper() and (not contiguous or lower_around(i)):
                parts.append(s[start: i])
                start = i
        parts.append(s[start:])
        return (" ").join(parts)

    #/************************************************************************/
    @staticmethod
    def join(strings, delim = ' '):
        """Join method that deals with empty strings

        Example
        -------
            >>> strings = ['a','  ','b', None, 'c']
            >>> Text.join(strings, delim = ', ')
                'a, b, c'
        """
        return delim.join(filter(lambda s: (s or '').strip(),
                                 [s.strip() for s in strings]))

    #/************************************************************************/
    @staticmethod
    def sub_patterns(strings, pattern):
        """Return a list of string matching a certain pattern in a list of strings.

            >>> res = sub_patterns(strings, pattern)
        """
        if pattern in (None,'','(.*)'):
            return strings
        else:
            return [s.group() for s in \
                    [re.search(r''+ pattern, st, re.M|re.I) for st in strings] if s]

    #/************************************************************************/
    @staticmethod
    def where_match(pattern, strings):
        """Return the indexes of the elements in a list of strings that match a given
        pattern.

            >>> resi = where_match(pattern, strings)

        Note
        ----
        see also :meth:`first_match`, :meth:`first_non_match`\ .
        """
        return [re.search(pattern, i) for i in strings]

    #/************************************************************************/
    @staticmethod
    def first_match(pattern, strings):
        """Return the index of the first element in a list of strings that matches a
        given pattern, or the lenght of that list if no match was found.

            >>> resi = first_match(pattern, strings)

        Note
        ----
        see also :meth:`first_non_match`, :meth:`where_match`\ .
        """
        try:
            first = next(i for i, v in enumerate(TextProcess.where_match(pattern, strings)) if v)
        except:
            first = len(strings) #None
        return first

    #/************************************************************************/
    @staticmethod
    def first_non_match(pattern,list_string):
        """Return the index of the first element in a list of strings that does not
        match a given pattern, or the lenght of that list if all strings match.

            >>> resi = first_non_match(pattern, strings)

        Note
        ----
        see also :meth:`first_match`, :meth:`where_match`\ .
        """
        try:
            return next(i for i, v in enumerate(TextProcess.where_match(pattern,list_string)) if not v)
        except:
            first = len(list_string) #None
        return first

