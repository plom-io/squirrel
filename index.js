var async = require('async')
  , os = require('os')
  , util = require('util')
  , clone = require('clone')
  , events = require("events")
  , sutil = require('./lib/sutil');

function Squirrel(dpkg, options) {
  options = options || {};

  this.dpkg = clone(dpkg);
  events.EventEmitter.call(this);

  this.root = options.root || path.resolve();
  this.concurrency = options.concurrency || os.cpus().length;
};

util.inherits(Squirrel, events.EventEmitter);


Squirrel.prototype.start = function(callback){
  var that = this;

  async.eachSeries(that.dpkg.pipeline, function(stage, cb){
    if('map' in stage){
      that.map(stage.map, function(err){
        if(err) return cb(err);
        if('reduce' in stage){
          that.reduce(stage.reduce, cb);
        } else {
          cb(null);
        }
      });
    } else if ('run' in stage) {
      that.run(stage.run, cb);
    } else {
      cb(new Error('invalid pipeline'));
    }
    
  }, function(err){
    return callback(err, clone(that.dpkg));
  });
  
};


Squirrel.prototype.map = function(map, callback){
  map = clone(map);
  var that = this;

  async.eachSeries(map, function(step, cb){
    that._init(step, function(err){
      if(err) return cb(err);
      var batch = that._makeTasksMap(step);
      async.eachLimit(batch, that.concurrency, that._runTask.bind(that), function(err){
        if(err) return callback(err);
        that._package(step, cb);
      });
    });
  }, function(err){
    if(err) return callback(err);
    that._cleanUp(map, callback);
  });
};

Squirrel.prototype.reduce = function(reduce, callback){
  reduce = clone(reduce);
  var that = this;

  async.eachSeries(reduce, function(step, cb){
    that._init(step, function(err){
      if(err) return cb(err);
      var task = that._makeTaskReduce(step);
      that._runTaskAndPackage(task, step, cb);
    });
  }, function(err){
    if(err) return callback(err);    
    that._cleanUp(reduce, callback);
  });
};

Squirrel.prototype.run = function(run, callback){
  run = clone(run);
  var that = this;

  async.eachLimit(run, that.concurrency, function(step, cb){
    that._init(step, function(err){
      if(err) return cb(err);
      var task = that._makeTaskRun(step);
      that._runTaskAndPackage(task, step, cb);
    });
  }, function(err){
    if(err) return callback(err);
    that._cleanUp(run, callback);
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
