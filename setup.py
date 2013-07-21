"""
Kangaroo
--------

Light, concurrent and object oriented storage system.

"""
from setuptools import setup, find_packages


setup(
    name='Kangaroo',
    version='0_not_functional',
    url='https://github.com/carrerasrodrigo/Kangaroo.py',
    license='mit',
    author='Rodrigo N. Carreras',
    author_email='carrerasrodrigo@gmail.com',
    description='Light, concurrent and object oriented storage system.',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    classifiers=[]
)
