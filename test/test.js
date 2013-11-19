var util = require('util')
  , fs = require('fs')
  , assert = require('assert')
  , path = require('path')
  , smap = require('../lib/map')
  , rimraf = require('rimraf')
  , Squirrel = require('..');

var root = path.dirname(__filename);

describe('map', function(){
  var dpkg, map, options;

  before(function(){
    dpkg = JSON.parse(fs.readFileSync(path.join(root, 'data', 'package.json')));
    map = dpkg.pipeline[0];
    options = {root: root};
    inputs = map.data[0].inputs;
  });

  it('should add paths to inputs', function(done){    
    smap.addPaths(inputs, dpkg.resources, options, function(err, res){
      if (err) throw err;
      assert.deepEqual(res[0].paths, [path.resolve(root, 'data','lhs.json')]);
      done();
    });
  });

  it('should add data and id to inputs', function(done){    
    smap.addPaths(inputs, dpkg.resources, options, function(err, inputs){
      smap.addData(inputs, function(err, res){
        if (err) throw err;
        assert.deepEqual(JSON.parse(res[0].data[1].input.toString()), {"resources": [{"name": "b"}]});
        assert.equal(res[0].data[0].id, '0');
        assert.equal(res[0].data[1].id, '1');
        done();
      });
    });
  });

  it('should create a batch of task', function(done){    
    smap.addPaths(inputs, dpkg.resources, options, function(err, inputs){
      smap.addData(inputs, function(err, res){
        map.data[0].inputs = res;

        var batch = smap.makeTaskBatch(map.data[0], options);

        assert.equal(batch[0].command, path.resolve(root, "bin/bintestjs"));
        assert.equal(batch[0].options.cwd, path.resolve(root, "bin"));
        assert.equal(batch[0].walltime, 60*1000);
        assert.deepEqual(batch[0].args, [ path.resolve(root, 'data'), '0', 'X']);
        assert.deepEqual(batch[1].args, [ path.resolve(root, 'data'), '1', 'X']);
        
        done();
      });
    });
  });

  it('should work', function(done){
    var squirrel = new Squirrel(dpkg);

    squirrel.runMap("test-pipeline", options, function(err, dpkg){
      if (err) throw err;
      assert.deepEqual(dpkg.resources, [ 
        { name: 'lhs', path: 'data/lhs.json', format: 'json' },
        { name: 'X_0', path: path.resolve(root, 'data', 'X_0.csv'), format: 'csv' },
        { name: 'X_1', path: path.resolve(root, 'data', 'X_1.csv'), format: 'csv'  },
        { name: 'trace_0', path: path.resolve(root, 'data', 'subdata', 'my_trace_0.csv'), format: 'csv'  },
        { name: 'trace_1', path: path.resolve(root, 'data', 'subdata', 'my_trace_1.csv'), format: 'csv'  }
      ]);

      for(var i=1; i<dpkg.resources.length; i++){
        assert(fs.existsSync(dpkg.resources[i].path));
      }

      done();
    });
  });

  after(function(){
    rimraf.sync(path.resolve(root, 'data', 'subdata'));
    fs.unlinkSync(path.resolve(root, 'data', 'X_0.csv'));
    fs.unlinkSync(path.resolve(root, 'data', 'X_1.csv'));
  });

});
