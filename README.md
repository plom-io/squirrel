squirrel
========

Runs stuff in batch from some datapackage.json metadata, give them some inputs and package the outputs adding resources to the datapackage.

Usage
=====

    var Sqrl = require('dpkg-squirrel');
    var dpgk = require('datapackage.json);

    var sqrl = new Sqrl();
    sqrl.runMap(dpkg, {concurrency: 4}, function(err, dpkg){
       //everything has been run;  dpkg.ressources has been appended
    });
    sqrl.on("step", function(data){console.log('pipeline step completed (%s)', data)});

Tests
=====

    npm test


Licence
=======

MIT
