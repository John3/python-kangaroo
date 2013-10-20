Kangaroo.py
========

Kangaroo is light, object oriented storage system. 


Example
-------
::

    >>> from kangaroo.bucket import Bucket
    >>> bucket = Bucket()
    >>> bucket.new_table.insert(dict(animal="lion"))
    {'animal': 'lion'}
    >>> row = bucket.new_table.find(animal="lion")
    >>> print row.animal
    lion
    >>>


Installation 
------------
::

    > git clone git://github.com/carrerasrodrigo/python-kangaroo
    > cd python-kangaroo
    > python setup.py install



History
-------
**25/10/2013**
 - Added support for python3.2+ and python3.3+
 
**21/09/2013**
 - Added CVS Storage
 
**28/07/2013**
 - Added support for indexs
 - Added support for Json and Pickle Storages

**21/07/2013**
 - Inicial commit
