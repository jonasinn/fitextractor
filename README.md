=========
fitextractor
=========

A tool to put  `.fit <https://developer.garmin.com/fit/overview/>` files in a sql database (currently sqlite and postgres).

Usage Example
=============

Read a FIT file, frame by frame:

.. code:: python

    import glob
    from fitextractor import MultiFitProcessor

    filenames = glob.glob("fit_files/*.fit")[:10]

    db_url = 'sqlite:///test.db'
    mfe = MultiFitProcessor(filenames, db_url, multiprocessing=False)

    mfe.to_db(drop_tables=True)


Installation
============

Currently you can install it from github

    $ pip install git+git@github.com:jonasinn/fitextractor.git


Why?
========

Being tired of bad or data visualization platforms I thought the logical step was to load all my fitfiles in to a database and see if I could do something nicer for myself.

Currently the module is not extensively tested or optimized in any way, I simply got it to where I need it at the moment to be able to import the stack of files I have lying around.

License
=======

This project is distributed under the terms of the MIT license.
See the `LICENSE.txt <LICENSE.txt>`_ file for details.


Credits
=======

This would not have been possible without the existing fit file parsing libraries:

.. _fitdecode: https://github.com/polyvertex/fitdecode
.. _fitparse: https://github.com/dtcooper/python-fitparse