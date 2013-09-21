Kangaroo.py
========

Kangaroo is light, concurrent (not yet!) and object oriented storage system. 


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


Status
------------------
Currently it's under development. There it's not and stable version yet.


History
-------
**21/09/2013**
 - Added CVS Storage
 
**28/07/2013**
 - Added support for indexs
 - Added support for Json and Pickle Storages

**21/07/2013**
 - Inicial commit
