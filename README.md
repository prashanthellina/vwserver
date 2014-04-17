# vwserver

A python based RPC server that encapsulates the functionality of Vowpal Wabbit making it easier
to integrate it into your data workflow. The RPC protocol is language agnostic so you can
access from any kind of platform.

![Image](./vwserver_screenshot.png?raw=true)

## Installation

``` bash
sudo pip install git+git://github.com/prashanthellina/vwserver.git
```

## Usage

Start a VWServer by doing. Note that VWServer will use the specified directory "data" for storing all of its data. Make sure you do not have any of your own data there. You can change the port number to anything else you desire.

``` bash
vwserver --port 8889 data
```

Now the server is running and is web accessible. Open http://localhost:8889 in the browser. You will see a web-based python console to interact with this VWServer.

Try running the following commands and observe the output

``` python
>>> dir()
['__builtins__', 'api', 'call', 'logging', 'server']

>>> help(api)
Help on VWAPI in module vwserver.vwserver object:
 
class VWAPI(__builtin__.object)
 |  Methods defined here:
 |  
 |  __init__(self, data_dir)
 |  
 |  create(self, name, options=None)
 |      Creates a new VW model with @name and using @options
 |  
 |  destroy = wfn(self, vw, *args, **kwargs)
 |  
 |  predict = wfn(self, vw, *args, **kwargs)
 |  
 |  save = wfn(self, vw, *args, **kwargs)
 |  
 |  shutdown(self)
 |      Stop the server
 |  
 |  train = wfn(self, vw, *args, **kwargs)
 |  
 |  unload(self, vw)
 |      Unloads a VW model from memory. This does not
 |      destroy the model from disk and so it can be
 |      loaded again later for usage.
 |  
 |  ----------------------------------------------------------------------
 |  Data descriptors defined here:
 |  
 |  __dict__
 |      dictionary for instance variables (if defined)
 |  
 |  __weakref__
 |      list of weak references to the object (if defined)
```
 
 Now let us check if there are any models currently loaded (We expect to see none as we are just starting off)
 
``` python
>>> api.vws()
{}
```
 
 Let us create a new Vowpal Wabbit model.
 
``` python
>>> api.create('foobar')
```
 
And that's it! We are ready to use this for training and prediction. Try it out - You can refer to the image at the top of this document for a reference on how to perform training and prediction.

Note that we can pass options while creating a new VW model using the create method. Here is an example.

``` python
>>> api.create('foobar1', {'bit_precision': 20})
```

If you want to see what options are available and their default values, do

``` python
>>> api.show_options()
```

These options are directly converted into command line parameters for the vw command.

## Accessing using a remote client
