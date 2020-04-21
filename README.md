pyeudatnat
=========

Basic module (`Python`) for the automated collection and formatting of raw EU data from national authorities based on provider's metadata.
---

**Quick install and start**

[![Binder](https://mybinder.org/badge_logo.svg)](http://mybinder.org/v2/gh/eurostat/pyEUDatNat/master?filepath=notebooks)

Once installed, the module can be imported simply:

```python
>>> import pyeudatnat
```

**Notebook examples**

* A [basic example](https://nbviewer.jupyter.org/github/eurostat/healthcare-services/blob/master/src/geo-py/notebooks/01_HCS_basic_example.ipynb) regarding healthcare services to start with the module.
* ...

**Usage**

###### Manual setting

You will need first to create a special class given the metadata associated each 
the national data:

```python
>>> from pyeudatnat import base
>>> NewDataCategory = base.datnatFactory(category = 'new')
```

Following, it is pretty straigthforward to create an instance of a national dataset:

```python
>>> datnat = NewDataCategory()
>>> datnat.load_data()
>>> datnat.format_data()
>>> datnat.save_data(fmt = 'csv')
```

Note the output schema (see also "attributes" in the documentation [below](#Data)) should be defined outside, _e.g._ in an external [`config.py`](config.py) file.

<!-- .. ` -->
###### Features

* Various possible geocoding, including `GISCO`.

Default coder is `GISCO`, but you can use a different geocoder also using an appropriate key:

* Automated translation,
* ...
 
**<a name="Software"></a>Software resources/dependencies**

* Packages for data handling: [`pandas`](http://pandas.pydata.org).
* Packages for geocoding:  [`geopy`](https://github.com/geopy/geopy), [`pyproj`](https://github.com/pyproj4/pyproj) and [`happygisco`](https://github.com/eurostat/happyGISCO).
* Package for JSON formatting:  [`geojson`](https://github.com/jazzband/geojson).
* Package for translations:  [`googletrans`](https://github.com/ssut/py-googletrans).
<!-- * Packages for map visualisations: [`ipyleaflet`](https://github.com/jupyter-widgets/ipyleaflet) or [`folium`](https://github.com/python-visualization/folium). -->

