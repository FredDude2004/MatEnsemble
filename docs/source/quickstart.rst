===========
Quick Start
===========

MatEnsemble ships updated Docker containers with all its dependencies packaged
and ready for use. To start using MatEnsemble you can pull one of our containers
down from the [registry](https://github.com/FredDude2004/MatEnsemble/pkgs/container/matensemble). MatEnsemble has three different tags with each release. 

Tags
^^^^

*       matensemble:baseline-vX.Y.Z
*       matensemble:frontier-vX.Y.Z
*       matensemble:perlmutter-vX.Y.Z

Each tag is optimized for a specific HPC system. The baseline image is the most
general and portable container but it lacks the *hardware specific* GPU and MPI
capabilites that come with the other two images. 

Apptainer
---------

Many HPC systems come with a version of Apptainer (formerly Signularity) installed. 
Building MatEnsemble with apptainer is very simple. 

.. code-block:: bash

   apptainer build matensemble.sif ghcr.io/freddude2004/matensemble:<tag>

.. note:: 
   We should put an example batch script here with commands

PodMan-HPC
----------

.. note:: 
   For systems with PodMan-HPC instructions would go here. 


Frontier
--------

Put more detailed Frontier specific instructions here

Perlmutter
----------

Put more detailed Perlmutter specific instructions here

MatEnsemble is available on PyPI and can be installed with pip

.. code-block:: bash

   pip install matensemble

MatEnsemble needs two pieces of the [flux-framework](https://flux-framework.readthedocs.io/en/latest/) installed to run. If you are on

