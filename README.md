squirrel
========

Runs stuff in batch from some datapackage.json metadata, give them some inputs and package the outputs adding resources to the datapackage.

[![NPM](https://nodei.co/npm/dpkg-squirrel.png)](https://nodei.co/npm/dpkg-squirrel/)

Usage
=====

    var Sqrl = require('dpkg-squirrel');
    var dpgk = require('datapackage.json);

    var sqrl = new Sqrl(dpkg);
    sqrl.runMap(name, {concurrency: 4}, function(err, dpkg){
       //everything has been run;  dpkg.ressources has been appended
    });
    sqrl.on("step", function(step){console.log('pipeline step completed (%s)', step)});


Tests
=====

    npm test


Licence
=======

MIT
