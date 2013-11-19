var async = require('async')
  , os = require('os')
  , util = require('util')
  , clone = require('clone')
  , events = require("events")
  , smap = require('./lib/map');

function Squirrel(dpkg) {
  this.dpkg = dpkg;
  events.EventEmitter.call(this);
}

util.inherits(Squirrel, events.EventEmitter);

/**
 * dpkg a datapackage
 * map the map object to run
 * options: a hash with: root, concurency.
 *
 * callback: err, dpgk where dpkg.resources have been appended to take
 * into account the generated resources.
 */
Squirrel.prototype.runMap = function(nameMap, options, callback){

  var map = this.dpkg.pipeline.filter(function(x){return x.type==='map' && x.name === nameMap;})[0];
  if(!map){
    return callback(new Error('no map with this name: ' + nameMap));
  }

  var that = this;

  var dpkg = clone(this.dpkg);

  options.root = options.root || process.cwd();
  options.concurrency = options.concurrency || os.cpus().length;

  //create the queue
  var q = async.queue(function (task, cb) {
    smap.runTask(task, that, cb);
  }, options.concurrency);

  var data = clone(map.data);
  var i=0, j=0;

  (function pushTask(){
    
    smap.addPaths(data[i].inputs, dpkg.resources, options, function(err){
      if(err) return callback(err);

      smap.addData(data[i].inputs, function(err){
        if(err) return callback(err);

        var taskBatch = smap.makeTaskBatch(data[i], options);
        
        if(!taskBatch.length){
          if(++i <data.length){
            pushTask();
          } else {
            callback(null, dpkg);
          }
        } else {

          smap.mkdirpOutputs(data[i].outputs, options, function(err){
            if(err) return callback(err);

            j=0;
            q.push(taskBatch, function(err){
              if(err) return callback(err);

              if (++j >= taskBatch.length){
                smap.pkgAndMvOutputs(data[i].outputs, dpkg, options, function(err){
                  if(err) return callback(err);

                  if (++i <data.length) {
                    pushTask();
                  } else {
                    smap.cleanUp(data, dpkg, options, function(err){
                      if(err) return callback(err);
                      callback(null, dpkg);
                    });
                  }
                });
              }

            });
          });

        }

      });
    });
    
  })();

};

module.exports = Squirrel;
