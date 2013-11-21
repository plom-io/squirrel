var fs = require('fs')
  , path = require('path')
  , util = require('util')
  , async = require('async')
  , minimatch = require("minimatch")
  , glob = require('glob')
  , clone = require('clone')
  , spawn = require('child_process').spawn
  , mkdirp = require('mkdirp');


/**
 * For each input of inputs, adds a _paths property containing a list of
 * path corresponding to pattern described in inputs
 */
function addPaths(inputs, callback){

  var resources = this.dpkg.resources;
  var that = this;

  async.map(inputs, function(input, cb){
    var names, paths;

    if('resource' in input){ 

      names = resources
        .map(function(x){return x.name})
        .filter(minimatch.filter(input.resource, {matchBase: true}));

      paths = resources
        .filter(function(x){return names.indexOf(x.name) !== -1})
        .map(function(x){return path.resolve(that.root, x.path)});

      cb(null, paths);
    } else if ('path' in input){ 

      glob(path.resolve(that.root, input.path), {matchBase: true}, cb);
    } else {
      
      cb(new Error('invalid input'));
    }
  }, function(err, pathss){
    if (err) return callback(err);
    
    inputs.forEach(function(input, i){
      input._paths = pathss[i];
    });    

    callback(null, inputs);

  });

};

exports.addPaths = addPaths;


/**
 * inputs MUST already have an extra _paths property we
 * add an extra _data property mapping _paths to: [{id:, data:, path}, ...]
 * id is added if "*" was resolved to something or if it was specified in the input
 * data is added if stdin==true && (collection==true || replicate === true)
 * path is added if stdin==true
 */
function addIdAndStdinData(inputs, callback){
  var i = 0;
  (function loader(){

    async.map(inputs[i]._paths, function(path, cb){
    
      //in these case we need to read the file and store it. id is resolve to the collection or replication index
      if(inputs[i].stdin && (inputs[i].collection || inputs[i].replicate)){

        fs.readFile(path, function(err, data){
          if(err) return cb(err);

          try{
            data = JSON.parse(data);
          } catch(e){
            return cb(e);        
          }

          var arrayOf_dataObj = [];
          if(inputs[i].collection) {
            arrayOf_dataObj = data.map(function(x, id){
              return {id: id.toString(), data: JSON.stringify(x)};
            });
          } else { //replicate
            for(var j=0; j< inputs[i].replicate; j++){
              arrayOf_datadataObj.push({id:j.toString(), data: data});
            }
          }

          cb(null, arrayOf_dataObj);
        });

      } else {

        var _dataObj = {};
        if('path' in inputs[i]){
          var re = new RegExp(inputs[i].path.replace('*', '(.*)'));
          var match = path.match(re);
          if(match && match[1]){
            _dataObj.id =match[1];
          }          
          if(inputs[i].stdin){
            _dataObj.path = path;
          }
        }
        
        cb(null, _dataObj);
      }
      
    }, function(err, data){ //data is [arrayOf_dataObj] or [_dataObj, _dataObj, ...]

      if(err) return callback(err);
      inputs[i]._data = (inputs[i].collection || inputs[i].replicate)? data[0]: data; //if it's collection or replicate => inputs[i]._paths.length was 1

      if(++i < inputs.lenght){
        loader();
      } else {
        callback(null, inputs);
      }

    });
    
  })();

};

exports.addIdAndStdinData = addIdAndStdinData;


/**
 * make output directories for running computation *and * stored
 * results
 */
function mkdirpOutputs(outputs, callback){

  var that = this;

  var dirs = [];
  outputs.forEach(function(x, i){
    dirs.push(path.dirname(path.join(that.root, x.path)));
    if( ('save' in x) && ('path' in x.save)){
      dirs.push(path.dirname(path.join(that.root, x.save.path)));
    }
  });

  async.each(dirs, mkdirp, callback);

};

exports.mkdirpOutputs = mkdirpOutputs;


function _init(step, callback){
  var that = this;
  
  addPaths.call(that, step.inputs, function(err){
    if(err) return callback(err);
    addIdAndStdinData.call(that, step.inputs, function(err){
      if(err) return callback(err);
      mkdirpOutputs.call(that, step.outputs, callback);
    });
  });
};
exports._init = _init;



function _makeBatchObj(step){
  var that = this;

  var walltime = step.walltime || "00:01:00";
  walltime = walltime.split(':');
  walltime = (parseFloat(walltime[0], 10)*60*60 + parseFloat(walltime[1], 10)*60 + parseFloat(walltime[2], 10))*1000; //convert in millisec

  var command = (step.cwd) ? path.resolve(that.root, step.cwd, step.command): step.command;
  var opts = (step.cwd) ? {cwd: path.resolve(that.root,step.cwd)}: {};

  //resolve $ROOT in args
  var args = step.args.map(function(arg){ 
    if(arg.toString().indexOf('$ROOT') !== -1){
      var x = arg.replace('$ROOT', '');
      arg =  path.join(that.root, x);
    }
    return arg;
  });

  return {
    command: command,
    args: args,
    options: opts,
    walltime: walltime
  };

};


/**
 * convert in a format adapted to require('child_process').spawn
 * step is an elment of the map or reduce list
 */
function _makeTasksMap(step){

  var that = this;

  var batch = [];  
  var batchObj;  

  for(var i=0; i<step.inputs.length; i++){ 
    for(var j=0; j<step.inputs[i]._data.length; j++){
      batchObj = _makeBatchObj.call(that, step);
      batchObj.args = batchObj.args.map(function(arg){              
        if (arg.toString().indexOf('*') !== -1){
          arg = arg.replace('*', step.inputs[i]._data[j].id);
        }            
        return arg;
      });
      batchObj.toStdin = step.inputs[i]._data[j],
      batch.push(batchObj);
    }
  }

  return batch;
};

exports._makeTasksMap = _makeTasksMap;


function _makeTaskReduce(step, options){

  var batchObj = _makeBatchObj.call(this, step);    
  
  //resolve star to list of files in args
  var all_paths = [];
  if(batchObj.args.indexOf("*") !== -1){

    step.inputs.forEach(function(input){
      input._paths.forEach(function(_path){
        all_paths.push(_path);
      });
    });

    //replace "*" by the list of files in batchObj.args.
    var args = [];
    for(var i=0; i<batchObj.args.length; i++){
      if(batchObj.args[i] === "*"){        
        for(var j=0; j< all_paths.length; j++){
          args.push(all_paths[j]);          
        }
      } else{
        args.push(batchObj.args[i]);
      }
    }

    batchObj.args = args;    
  }

  return batchObj;
};

exports._makeTaskReduce = _makeTaskReduce;


function _makeTaskRun(step){
  var batchObj = _makeBatchObj.call(this, step);    

  if(step.inputs.length===1 && step.inputs[0].stdin && step.inputs[0]._paths.length===1){
    batchObj.toStdin = step.inputs[0]._data[0];
  }

  return batchObj;
};
exports._makeTaskRun = _makeTaskRun;



function _runTask(task, callback){

  var that = this;
  var hasCallbacked = false;

  var prog = spawn(task.command, task.args, task.options);
  prog.on('error', function(err){
    hasCallbacked = true;
    callback(err);
  });

  prog.stdout.resume(); // start the flow of data, discarding it.
  //prog.stdout.pipe(process.stdout);

  if(task.toStdin && task.toStdin.data){
    prog.stdin.write(task.toStdin.data+'\n', encoding="utf8");
  } else if (task.toStdin && task.toStdin.path){
    var s = fs.createReadStream(task.toStdin.path);
    s.pipe(prog.stdin);
  }

  var timeoutId = setTimeout(function(){        
    that.emit('wrn', util.format('walltime overshoot for task %j', task));
    prog.kill();
  }, task.walltime);

  prog.on('exit', function (code) {
    clearTimeout(timeoutId);   

    if(!hasCallbacked){
      if (code !== 0) {
        callback(new Error(util.format('task %s\nexited with code %d', task.command + ' ' + task.args.join(' '), code)));
      } else {
        that.emit('step', util.format('task %s\nexited with code %d', task.command + ' ' + task.args.join(' '), code));
        callback(null);
      }
    }

  });

};

exports._runTask = _runTask;




/**
 * This function suppose that all the directories necessary already
 * exist
 */
function _package(step, callback){

  var that = this;
  var outputs = step.outputs;

  //for each output that need to be saved
  async.each(outputs.filter(function(x){ return ('save' in x); }), function(output, cb){
    
    //get files from run that succeeded
    glob(path.resolve(that.root, output.path), {matchBase: true}, function(err, paths){
      if(err) return cb(err);
      if(!paths.length) return cb(null);

      //add entry to datapackage
      var re = new RegExp(output.path.replace('*', '(.*)'));
      
      paths.forEach(function(x){
        var id = x.match(re)[1];

        var savePath = (output.save.path && path.resolve(that.root, output.save.path.replace('*', id))) || x;
        var name = output.save.resource.replace('*', id);
        
        //remove resource if it already exists before (re-)pushing it
        var ind;
        for (var i=0; i< that.dpkg.resources.length; i++){
          if(that.dpkg.resources[i].name === name){
            ind = i; 
            break;
          }
        }
        if(typeof ind !== 'undefined'){
          that.dpkg.resources.splice(ind, 1);
        }

        that.dpkg.resources.push({
          name: name,
          path: savePath,
          format: path.extname(savePath).slice(1)
        });
      });

      //mv outputs (if needed)
      if(('path' in output.save) && (output.save.path !== output.path )){
        
        var mv = paths.map(function(x){
          var id = x.match(re)[1];
          return {
            oldPath: x,
            newPath:  path.resolve(that.root, output.save.path.replace('*', id))
          };
        });

        async.each(mv, function(item, cb2){
          fs.rename(item.oldPath, item.newPath, cb2);
        }, cb);

      } else {
        return cb(null);
      }

    });           

  }, callback);   
  
};

exports._package = _package;

function _runTaskAndPackage(task, step, callback){
  var that = this;

  that._runTask(task, function(err){
    if(err) return callback(err);    
    that._package(step, callback);
  });

};

exports._runTaskAndPackage = _runTaskAndPackage;



/**
 * delete all the outputs non saved
 * stepmr is either a map array or a reduce array
 */
function _cleanUp(stepmr, callback){
  var that = this;

  var i = 0;

  (function stepmrWalker(){
    async.each(stepmr[i].outputs, function(out, cb){
      if(!('save' in out) || ( ('save' in out) && ('path' in out.save) && (out.save.path !== out.path))){
        glob(path.resolve(that.root, out.path), {matchBase: true}, function(err, paths){
          if(err) return cb(err);
          async.each(paths, fs.unlink, cb);
        });
      } else {
        cb(null);
      }

    }, function(err){
      
      if(err){
        that.emit('error', err);
      }

      if(++i<stepmr.length){
        stepmrWalker();
      }else {
        callback(null);
      }

    });

  })();
  
};

exports._cleanUp = _cleanUp;
