var fs = require('fs')
  , path = require('path')
  , util = require('util')
  , async = require('async')
  , minimatch = require("minimatch")
  , glob = require('glob')
  , clone = require('clone')
  , spawn = require('child_process').spawn
  , exec = require('child_process').exec
  , mkdirp = require('mkdirp');

/**
 * For each input of inputs, adds a _paths property containing a list of
 * path corresponding to pattern described in input
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

          var arrayOf_dataObj = [];
          if(inputs[i].collection) {
            try{
              data = JSON.parse(data);
            } catch(e){
              return cb(e);        
            }
            arrayOf_dataObj = data.map(function(x, id){
              return {id: id.toString(), data: JSON.stringify(x)};
            });
          } else { //replicate
            for(var j=0; j< inputs[i].replicate; j++){
              arrayOf_dataObj.push({id:j.toString(), data: data});
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


function outResolver(outputs){
  var that = this;
  var res = {};
  outputs.forEach(function(out, i){
    if('name' in out){
      if(out.path.indexOf('*') !== -1){
        throw new Error('$OUT.' + out.name + ' cannot be used if the path is a glob');
      }
      res['$OUT.' + out.name] = path.resolve(that.root, out.path); //output must have a path (by definition)
    }
  });
  return res;
};

function inResolver(inputs){
  var res = {};
  inputs.forEach(function(inp, i){
    if('name' in inp){      
      if((inp._paths.length !== 1) ||  (inp._paths[0].indexOf('*') !== -1)) {
        throw new Error('$IN.' + inp.name + ' cannot be used if the path is a glob');
      }
      res['$IN.' + inp.name] = inp._paths[0]; //input might not have a path (if resource) but _path has been added
    }
  });
  return res;
};


function _makeBatchObj(step){
  var that = this;

  var options = {
    cwd: (step.cwd) ? path.resolve(that.root, step.cwd) : that.root
  };

  options.timeout = step.timeout || "00:01:00";
  options.timeout = options.timeout.split(':');
  options.timeout = (parseFloat(options.timeout[0], 10)*60*60 + parseFloat(options.timeout[1], 10)*60 + parseFloat(options.timeout[2], 10))*1000; //convert in millisec

  var outRes = outResolver.call(that, step.outputs)
    , inRes = inResolver(step.inputs)
    , allPaths = [];

  step.inputs.forEach(function(input){
    input._paths.forEach(function(_path){
      allPaths.push(_path);
    });
  });

  var args;

  var command = step.command;
  if('args' in step){ //->spawn
    var _args = step.args.map(function(arg){ 
      //resolve $ROOT
      if(arg.toString().indexOf('$ROOT') !== -1){
        var x = arg.replace('$ROOT', '');
        arg =  path.join(that.root, x);
      }

      //resolve $OUT.smtg
      if(arg in outRes){
        arg = outRes[arg];
      }

      //resolve $IN.smtg
      if(arg in inRes){
        arg = inRes[arg];
      }

      return arg;
    });

    //resolve $IN (not $IN.smtg): has to be done in a second pass as it modifies the length of args to list of files in args
    args = [];
    if(_args.indexOf("$IN") !== -1){
      //replace "$IN" by the list of files in _args.
      for(var i=0; i<_args.length; i++){
        if(_args[i] === "$IN"){        
          for(var j=0; j< allPaths.length; j++){
            args.push(allPaths[j]);          
          }
        } else{
          args.push(_args[i]);
        }
      }
    } else {
      args = _args;
    }

  } else { //->exec we need to process the string to resolve $ROOT and $IN, $IN., $OUT.
    command = command.replace(/\$ROOT/g, that.root);
    command = command.replace(/\$IN /g, allPaths.join(' ') + ' ');
    [inRes, outRes].forEach(function(res){     //$IN. and $OUT.
      var re;
      for(var key in res){
        re = new RegExp("\\" + key);
        command = command.replace(re, res[key], 'g');
      }
    });
  }

  return {
    command: command,
    args: args, //if undefined => exec if not => spawn
    options: options
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

  //resolve $ID and feed stdin if needed
  for(var i=0; i<step.inputs.length; i++){ 
    for(var j=0; j<step.inputs[i]._data.length; j++){
      batchObj = _makeBatchObj.call(that, step);
      batchObj.args = batchObj.args.map(function(arg){              
        if (arg.toString().indexOf('$ID') !== -1){
          arg = arg.replace('$ID', step.inputs[i]._data[j].id);
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


/**
 * A reduce task cannot have stdin inputs
 */
function _makeTaskReduce(step, options){
  return _makeBatchObj.call(this, step);    
};

exports._makeTaskReduce = _makeTaskReduce;


function _makeTaskRun(step){
  var batchObj = _makeBatchObj.call(this, step);    

  //feed stdin if needed
  if(step.inputs.length===1 && step.inputs[0].stdin && step.inputs[0]._paths.length===1){
    batchObj.toStdin = step.inputs[0]._data[0];
  }

  return batchObj;
};
exports._makeTaskRun = _makeTaskRun;



function runTaskSpawn(task, callback){
  
  var that = this;
  var hasCallbacked = false;

  var prog = spawn(task.command, task.args, task.options);
  prog.on('error', function(err){
    if(!hasCallbacked){
      hasCallbacked = true;
      callback(err);
    }
  });  
  prog.stdout.on('error', function(err){
    if(!hasCallbacked){
      hasCallbacked = true;
      callback(err);
    }
  });
  prog.stdin.on('error', function(err){
    if(!hasCallbacked){
      hasCallbacked = true;
      callback(err);
    }
  });

  prog.stdout.resume(); // start the flow of data, discarding it.
  //prog.stdout.pipe(process.stdout);
  //prog.stderr.pipe(process.stdout);

  if(task.toStdin && task.toStdin.data){
    prog.stdin.write(task.toStdin.data+'\n', encoding="utf8");
  } else if (task.toStdin && task.toStdin.path){
    fs.createReadStream(task.toStdin.path).pipe(prog.stdin);   
  }

  var timeoutId = setTimeout(function(){        
    that.emit('wrn', util.format('timeout overshoot for task %j', task));
    prog.kill();
  }, task.options.timeout);

  prog.on('exit', function (code) {
    clearTimeout(timeoutId);   

    if(!hasCallbacked){
      if (code !== 0) {
        hasCallbacked = true;
        callback(new Error(util.format('task %s\nexited with code %d', task.command + ' ' + task.args.join(' '), code)));
      } else {
        that.emit('step', util.format('task %s\nexited with code %d', task.command + ' ' + task.args.join(' '), code));
        hasCallbacked = true;
        callback(null);
      }
    }

  });
  
};


function runTaskExec(task, callback){
  var that = this;

  var child = exec(task.command, task.options, function (err, stdout, stderr) {
    if(err) return callback(err);
    that.emit('step', util.format('task %s\nexited', task.command));
    return callback(null);
  });

};


function _runTask(task, callback){
  if(task.args){
    runTaskSpawn.call(this, task, callback);    
  } else {
    runTaskExec.call(this, task, callback);
  }
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
        
        if(that.dpkg.resources){
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
        } else {
          that.dpkg.resources = [];
        }

        that.dpkg.resources.push({
          name: name,
          path: savePath.replace(that.reRoot, '.'), //all the path have to be relative to the root of the datapackage.
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
 */
function _cleanUp(stage, callback){
  var that = this;

  var all = [];
  ['map', 'reduce', 'run'].forEach(function(type){
    if (type in stage){
      stage[type].forEach(function(step){
        all.push(step);
      });
    }
  });

  var i = 0;
  
  (function walker(){
    async.each(all[i].outputs, function(out, cb){
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
        that.emit('error', err.message);
      }

      if(++i<all.length){
        walker();
      }else {
        callback(null);
      }

    });

  })();
  
};

exports._cleanUp = _cleanUp;
