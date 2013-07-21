Kangaroo.py
========

Kangaroo is light, concurrent and object oriented storage system. 


Example
-------
::

    from kangaroo import Bucket
    bucket = Bucket()
    bucket.new_table.insert(dict(animal="lion"))
    bucket.new_table.find(animal="lion")


Status
------------------
Currently it's under development. There it's not and stable version yet.


History
-------
**21/07/2013**
 - Inicial commit
