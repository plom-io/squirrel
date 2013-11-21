squirrel
========

Runs stuff in batch from some data package metadata, give them some
inputs and package the outputs adding resources to the datapackage.

[![NPM](https://nodei.co/npm/dpkg-squirrel.png)](https://nodei.co/npm/dpkg-squirrel/)

Usage
=====

    var Sqrl = require('dpkg-squirrel');
    var dpgk = require('/path/to/data/package.json);

    var sqrl = new Sqrl(dpkg, {concurrency: 4});
    sqrl.start(function(err, newDpkg){
       //everything has been run;  newDpkg contains new resources
    });
    sqrl.on('step', function(msg){console.log(msg)});


Tests
=====

    npm test


Licence
=======

MIT
