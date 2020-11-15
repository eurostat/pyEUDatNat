#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.. __init__

Initiatlisation module for package `pyeudatnat` supporting the automated harvesting 
and ingestion of datasets made openly available by national/regional/local authorities.
"""

# *credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_ 
# *since*:        Sun Mar 29 16:21:29 2020

#                                                                         ,d                             ,d 
#                                                                         88                             88                                                       
# 8b,dPPYba,  8b       d8  ,adPPYba, 88       88  ,adPPYb,88 ,adPPYYba, MM88MMM 8b,dPPYba,  ,adPPYYba, MM88MMM 
# 88P'    "8a `8b     d8' a8P_____88 88       88 a8"    `Y88 ""     `Y8   88    88P'   `"8a ""     `Y8   88 
# 88       d8  `8b   d8'  8PP""""""" 88       88 8b       88 ,adPPPPP88   88    88       88 ,adPPPPP88   88 
# 88b,   ,a8"   `8b,d8'   "8b,   ,aa "8a,   ,a88 "8a,   ,d88 88,    ,88   88,   88       88 88,    ,88   88, 
# 88`YbbdP"'      Y88'     `"Ybbd8"'  `"YbbdP'Y8  `"8bbdP"Y8 `"8bbdP"Y8   "Y888 88       88 `"8bbdP"Y8   "Y888  
# 88              d8'                                                            
# 88             d8'                                                             
                                                                
#%% Settings

#import os, sys
#import warnings#analysis:ignore
#
#__SHOWWARNINGS      = True
#if not sys.warnoptions:
#    if __SHOWWARNINGS:
#        warnings.simplefilter("ignore",)
#        # warnings.filterwarnings("ignore")
#        # os.environ["PYTHONWARNINGS"] = "ignore" 
#    else:
#        warnings.simplefilter("default")
#        os.environ["PYTHONWARNINGS"] = "default" 

from os import path as osp

#%% Global vars

PACKNAME            = 'pyeudatnat' # this package...
"""Name of this package.
"""

#PACKPATH            = getattr(sys.modules[PACKNAME], '__path__')[0] # exactly: pygeofacil.__path__[0] #
PACKPATH            = osp.dirname(__file__) 
"""Path to this package.
"""

# METANAME            = 'meta'
# """Metabase generic name.
# """

# set the geographical area covered by the facilities
__AREA              = [ "EU28", "EFTA" ] # see __countries below: "EU27_2020", "EU27_2019", "EU27_2009"


## you shouldnt need to change anything below this

BASENAME            = '' 

__modules           = ['meta', 'misc', 'text', 'geo', 'base', 'harmonise', 'validate'] 

__all__             = ['%s%s' % (__,BASENAME) for __ in __modules]
__all__.extend(['__version__', '__start__'])

__ext_packages_ext  = ['numpy', 'pandas', 'json', 'datetime', 'geopy', 'geojson', 
                       'happygisco', 'pyproj', 'googletrans']

ISOCOUNTRIES        = { ## alpha-2/ISO 3166 codes
                        'BE': 'Belgium',                
                        'EL': 'Greece',                 
                        'LT': 'Lithuania',              
                        'PT': 'Portugal',               
                        'BG': 'Bulgaria',               
                        'ES': 'Spain',                  
                        'LU': 'Luxembourg',             
                        'RO': 'Romania',                
                        'CZ': 'Czechia',                
                        'FR': 'France',                 
                        'HU': 'Hungary',                
                        'SI': 'Slovenia',               
                        'DK': 'Denmark',                
                        'HR': 'Croatia',                
                        'MT': 'Malta',                  
                        'SK': 'Slovakia',               
                        'DE': 'Germany',                
                        'IT': 'Italy',                  
                        'NL': 'Netherlands',            
                        'FI': 'Finland',                
                        'EE': 'Estonia',                
                        'CY': 'Cyprus',                 
                        'AT': 'Austria',                
                        'SE': 'Sweden',                 
                        'IE': 'Ireland',                
                        'LV': 'Latvia',                 
                        'PL': 'Poland',                 
                        'UK': 'United Kingdom',         
                        'IS': 'Iceland',                
                        'NO': 'Norway',                 
                        'CH': 'Switzerland',            
                        'LI': 'Liechtenstein',          
                        'ME': 'Montenegro',             
                        'MK': 'North Macedonia',        
                        'AL': 'Albania',                
                        'RS': 'Serbia',                 
                        'TR': 'Turkey',                 
                        'XK': 'Kosovo',                 
                        'BA': 'Bosnia and Herzegovina', 
                        'MD': 'Moldova',                
                        'AM': 'Armenia',                
                        'BY': 'Belarus',                
                        'GE': 'Georgia',                
                        'AZ': 'Azerbaijan',             
                        'UA': 'Ukraine',                
                        }
"""Country ISO-codes.
"""

# ISOCODECTRIES = dict(map(reversed, ISOCOUNTRIES.items())) # {v:k for (k,v) in ISOCOUNTRIES.items()}

AREAS            = { "EU27_2020":
                            ["BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR", "HR", "IT", "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT", "RO", "SI", "SK", "FI", "SE"
                             ],
                        "EU27_2019": 
                            ["BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR", "HR", "IT", "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT", "RO", "SI", "SK", "FI", "SE"
                            ],
                        "EU28": 
                            ["BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR", "HR", "IT", "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT", "RO", "SI", "SK", "FI", "SE", "UK"
                             ],
                        "EU27_2009": 
                            ["BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR", "IT", "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT", "RO", "SI", "SK", "FI", "SE", "UK" 
                             ], 
                        "EFTA": 
                            ["CH", "IS", "NO", "LI"
                             ]
                        }    
__TERRITORIES       = { }

__AREA.extend(__TERRITORIES.keys())

#__COUNTRIES     = {a:__countries[a] for a in __area}
#__COUNTRIES.update({t:__territories[t] for t in __territories})
__COUNTRIES         = dict((k,v) for (k,v) in ISOCOUNTRIES.items() for a in __AREA if k in AREAS[a])
__COUNTRIES.update(dict((k,v) for (k,v) in ISOCOUNTRIES.items() for t in __TERRITORIES if k in __TERRITORIES[t]))

COUNTRIES           = __COUNTRIES
"""Countries belonging to the area covered by the facilities.
"""
            
try:
    # del(__SHOWWARNINGS)
    del(__AREA, __COUNTRIES, __TERRITORIES)
except: pass
