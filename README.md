squirrel
========

Runs stuff in batch from some datapackage.json metadata, give them some inputs and package the outputs adding resources to the datapackage.

Usage
=====

    var sqrl = require('dpkg-squirrel');
    var dpgk = require('datapackage.json);

    sqrl.runMap(dpkg, {concurrency: 4}, function(err, dpkg){
       //everything has been run;  dpkg.ressources has been appended
    });


Tests

    npm test


Licence

MIT
