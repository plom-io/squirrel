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
 * For each input of inputs, map an object with a pattern property
 * (optional) and a path property containing a list of path
 * corresponding to pattern
 */
function addPaths(inputs, resources, options, callback){

  async.map(inputs, function(input, cb){
    var names, paths;

    if('resource' in input){ 

      names = resources
        .map(function(x){return x.name})
        .filter(minimatch.filter(input.resource, {matchBase: true}));

      paths = resources
        .filter(function(x){return names.indexOf(x.name) !== -1})
        .map(function(x){return path.resolve(options.root, x.path)});

      cb(null, paths);
    } else if ('path' in input){ 

      glob(path.resolve(options.root, input.path), {matchBase: true}, function(err, paths){
        cb(err, paths);
      });
    } else {
      
      cb(new Error('invalid input'));
    }
  }, function(err, pathss){
    if (err) return callback(err);
    
    inputs.forEach(function(input, i){
      input.paths = pathss[i];
    });

    callback(null, inputs);

  });

};

exports.addPaths = addPaths;

/**
 * inputs have an extra paths (list of path property) we add an extra
 * data property ({id: input:})
 */
function addData(inputs, callback){
  var i = 0;
  (function loader(){

    async.map(inputs[i].paths, function(path, cb2){

      fs.readFile(path, function(err, data){
        if(err) return cb2(null, null);
        
        if (inputs[i].collection){
          try{
            data = JSON.parse(data);
          } catch(e){
            return cb2(e);        
          }

          data = data.map(function(x, i){
            return {id:i.toString(), input: JSON.stringify(x)};
          });

          cb2(null, data);

        } else {
          
          var re = new RegExp(inputs[i].path.replace('*', '(.*)'));
          var id = path.match(re)[1];

          cb2(null, {id: id, input: data});
        }
        
      });
      
    }, function(err, data){

      if(err) return callback(err);
      inputs[i].data = (inputs[i].collection)? data[0]: data;

      if(++i < inputs.lenght){
        loader();
      } else {
        callback(null, inputs);
      }

    });
    
  })();

};

exports.addData = addData;


function makeTaskBatch(step, options){

  var walltime = step.walltime || "00:01:00";
  walltime = walltime.split(':');
  walltime = (parseFloat(walltime[0], 10)*60*60 + parseFloat(walltime[1], 10)*60 + parseFloat(walltime[2], 10))*1000; //convert in millisec
  
  var batch = [];

  for(var i=0; i<step.inputs.length; i++){    
    for(var j=0; j<step.inputs[i].data.length; j++){
      if(step.inputs[i].data[j]){
        batch.push(
          {
            command: (step.cwd) ? path.resolve(options.root, step.cwd, step.command): step.command,
            args: step.args.map(function(arg){              
              if(arg.indexOf('$ROOT') !== -1){
                var x = arg.replace('$ROOT', '');
                arg =  path.join(options.root, x);
              } 
              if (arg.indexOf('*') !== -1){
                arg = arg.replace('*', step.inputs[i].data[j].id);
              }
              return arg;
            }),
            input: step.inputs[i].data[j].input,
            options: (step.cwd) ? {cwd: path.resolve(options.root,step.cwd)}: {},
            walltime: walltime
          }
        )
      }
    }
  }

  return batch;
};

exports.makeTaskBatch = makeTaskBatch;

/**
 * make output directories for running computation *and * stored
 * results
 */
function mkdirpOutputs(outputs, options, callback){

  var dirs = [];
  outputs.forEach(function(x, i){
    dirs.push(path.dirname(path.join(options.root, x.path)));
    if( ('save' in x) && ('path' in x.save)){
      dirs.push(path.dirname(path.join(options.root, x.save.path)));
    }
  });



  async.each(dirs, mkdirp, callback);

};

exports.mkdirpOutputs = mkdirpOutputs;


/**
 * This function suppose that all the directories necessary already
 * exist
 */
function pkgAndMvOutputs(outputs, dpkg, options, callback){

  async.each(outputs.filter(function(x){ return ('save' in x); }), function(output, cb){
    
    //get files from run that succeeded
    glob(path.resolve(options.root, output.path), {matchBase: true}, function(err, paths){
      if(err) return cb(err);
      if(!paths.length) return cb(null);

      //add entry to datapackage
      var re = new RegExp(output.path.replace('*', '(.*)'));
      var id;
      paths.map(function(x){
        id = x.match(re)[1];
        
        dpkg.resources.push({
          name: output.save.resource.replace('*', id),
          path: (output.save.path && path.resolve(options.root, output.save.path.replace('*', id))) || x
        });
      });


      //mv outputs (if needed)
      if(('path' in output.save) && (output.save.path !== output.path )){
        
        var mv = paths.map(function(x){
          id = x.match(re)[1];
          return {
            oldPath: x,
            newPath:  path.resolve(options.root, output.save.path.replace('*', id))
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

exports.pkgAndMvOutputs = pkgAndMvOutputs;




function runTask(task, callback){

  var prog = spawn(task.command, task.args, task.options);
  prog.on('error', console.log);

  prog.stdout.resume(); // start the flow of data, discarding it.

  prog.stderr.pipe(process.stdout);
  prog.stdout.pipe(process.stdout);

  prog.stdin.write(task.input+'\n', encoding="utf8");

  var timeoutId = setTimeout(function(){        
    console.error('\033[93mWARNING\033[0m: walltime overshoot for task %j', task);
    prog.kill();
  }, task.walltime);

  prog.on('exit', function (code) {
    clearTimeout(timeoutId);   

    if (code !== 0) {
      callback(new Error(util.format('task %s\nexited with code %d', task.command + ' ' + task.args.join(' '), code)));
    } else {
      console.log('\033[94mINFO\033[0m: task %s\nexited with code %d', task.command + ' ' + task.args.join(' '), code);
      callback(null);
    }
  });

};

exports.runTask = runTask;



/**
 * delete all the outputs non saved
 */
function cleanUp(map, dpkg, options, callback){

  var i = 0;

  (function mapWalker(){
    async.each(map[i].outputs, function(out, cb){
      if(!('save' in out) || ( ('save' in out) && ('path' in out.save) && (out.save.path !== out.path))){
        glob(path.resolve(options.root, out.path), {matchBase: true}, function(err, paths){
          if(err) return cb(err);
          async.each(paths, fs.unlink, cb);
        });
      } else {
        cb(null);
      }

    }, function(err){

      if(++i<map.length){
        mapWalker();
      }else {
        callback(null, dpkg);
      }

    });

  })();
  
};

exports.cleanUp = cleanUp;
