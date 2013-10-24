var async = require('async')
  , os = require('os')
  , util = require('util')
  , clone = require('clone')
  , events = require("events")
  , smap = require('./lib/map');


function Squirrel() {
  events.EventEmitter.call(this);
}

util.inherits(Squirrel, events.EventEmitter);


/**
 * dpkg a datapackage.json
 *
 * options: a hash with: root, concurency.
 *
 * callback: err, dpgk where dpkg.resources have been appended to take
 * into account the generated resources.
 */
Squirrel.prototype.runMap = function(dpkg, options, callback){

  var that = this;

  options.root = options.root || process.cwd();
  options.concurrency = options.concurrency || os.cpus().length;

  //create the queue
  var q = async.queue(function (task, cb) {
    smap.runTask(task, that, cb);
  }, options.concurrency);

  var data = clone(dpkg.analysis.filter(function(x){return x.name === 'map'})[0].data);
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
