var async = require('async')
  , os = require('os')
  , util = require('util')
  , clone = require('clone')
  , events = require("events")
  , sutil = require('./lib/sutil');

function Squirrel(dpkg, options) {
  options = options || {};

  events.EventEmitter.call(this);

  this.root = options.root || path.resolve();
  this.concurrency = options.concurrency || os.cpus().length;

  this.dpkg = clone(dpkg);
  
  //preprocessing of the pipeline: resolve the repeated step
  this.pipeline = clone(this.dpkg.pipeline);

  var that = this;
  this.dpkg.pipeline.forEach(function(stage, inds){
    ['map', 'reduce', 'run'].forEach(function(type){
      if(type in stage){
        var newSteps = [];
        stage[type].forEach(function(step){
          if ('repeat' in step){
            for(var i=0; i<step.repeat; i++){
              newSteps.push(clone(step));
              delete newSteps[newSteps.length-1].repeat;
            }
          } else {
            newSteps.push(clone(step));
          }
        });
        that.pipeline[inds][type] = newSteps;
      }
    });
  });

};

util.inherits(Squirrel, events.EventEmitter);


Squirrel.prototype.start = function(callback){
  var that = this;

  async.eachSeries(that.pipeline, function(stage, cb){
    if('map' in stage){
      that.map(stage, function(err){
        if(err) return cb(err);
        if('reduce' in stage){
          that.reduce(stage, cb);
        } else {
          cb(null);
        }
      });
    } else if ('run' in stage) {
      that.run(stage, cb);
    } else {
      cb(new Error('invalid pipeline'));
    }
    
  }, function(err){
    return callback(err, clone(that.dpkg));
  });
  
};


Squirrel.prototype.map = function(stage, callback){
  var map = clone(stage.map);
  var that = this;

  async.eachSeries(map, function(step, cb){
    that._init(step, function(err){
      if(err) return cb(err);
      try{
        var batch = that._makeTasksMap(step);
      } catch(e){
        return cb(e);
      }
      async.eachLimit(batch, that.concurrency, that._runTask.bind(that), function(err){
        if(err) return callback(err);
        that._package(step, cb);
      });
    });
  }, function(err){
    if(err || ('reduce' in stage)) return callback(err); //do not cleanUp if reduce

    that._cleanUp(stage, callback);
  });
};

Squirrel.prototype.reduce = function(stage, callback){
  var reduce = clone(stage.reduce);
  var that = this;

  async.eachSeries(reduce, function(step, cb){
    that._init(step, function(err){
      if(err) return cb(err);
      try {
        var task = that._makeTaskReduce(step);
      } catch(e){
        return cb(e);
      }
      that._runTaskAndPackage(task, step, cb);
    });
  }, function(err){
    if(err) return callback(err);    
    that._cleanUp(stage, callback);
  });
};

Squirrel.prototype.run = function(stage, callback){
  run = clone(stage.run);
  var that = this;

  async.eachLimit(run, that.concurrency, function(step, cb){
    that._init(step, function(err){
      if(err) return cb(err);
      try {
        var task = that._makeTaskRun(step);
      } catch(e){
        return cb(e);
      }
      that._runTaskAndPackage(task, step, cb);
    });
  }, function(err){
    if(err) return callback(err);
    that._cleanUp(stage, callback);
  });
};


module.exports = Squirrel;

Squirrel.prototype._init = sutil._init;
Squirrel.prototype._makeTasksMap = sutil._makeTasksMap;
Squirrel.prototype._makeTaskReduce = sutil._makeTaskReduce;
Squirrel.prototype._makeTaskRun = sutil._makeTaskRun;
Squirrel.prototype._runTask = sutil._runTask;
Squirrel.prototype._package = sutil._package;
Squirrel.prototype._runTaskAndPackage = sutil._runTaskAndPackage;
Squirrel.prototype._cleanUp = sutil._cleanUp;
