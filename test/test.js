var util = require('util')
  , fs = require('fs')
  , assert = require('assert')
  , path = require('path')
  , sutil = require('../lib/sutil')
  , rimraf = require('rimraf')
  , Squirrel = require('..');

var root = path.dirname(__filename);

describe('squirrel', function(){

  describe('repeat', function(){
    it('should resolve repeat', function(){    
      var dpkg = { pipeline: [ { run: [ { repeat: 3, command: 'a', inputs: [ { path: 'a' } ], outputs: [ { path: 'b' } ] } ] } ] };
      var squirrel = new Squirrel(dpkg, {root: root});
      assert.equal(squirrel.pipeline[0].run.length, 3);
    });
  });


  describe('pipeline', function(){
    var dpkg, step, inputs;
    var squirrel;

    var expectedResources = [
      { name: 'lhs', path: 'data/lhs.json', format: 'json' },
      { name: 'X_0', path: path.resolve(root, 'data', 'X_0.csv'), format: 'csv' },
      { name: 'X_1', path: path.resolve(root, 'data', 'X_1.csv'), format: 'csv'  },
      { name: 'trace_0', path: path.resolve(root, 'data', 'subdata', 'my_trace_0.csv'), format: 'csv'  },
      { name: 'trace_1', path: path.resolve(root, 'data', 'subdata', 'my_trace_1.csv'), format: 'csv'  },
      { name: 'summary', path: path.resolve(root, 'data', 'subdata', 'summary.json'), format: 'json'  }
    ];


    beforeEach(function(){
      dpkg = JSON.parse(fs.readFileSync(path.join(root, 'data', 'package.json')));
      step = dpkg.pipeline[0];
      inputs = step.map[0].inputs;
      squirrel = new Squirrel(dpkg, {root: root});
    });

    it('should add _paths to inputs', function(done){    
      sutil.addPaths.call(squirrel, inputs, function(err, res){
        if (err) throw err;
        assert.deepEqual(res[0]._paths, [path.resolve(root, 'data','lhs.json')]);
        done();
      });
    });

    it('should add _data  to inputs', function(done){    
      sutil.addPaths.call(squirrel, inputs, function(err, inputs){
        sutil.addIdAndStdinData.call(squirrel, inputs, function(err, res){
          if (err) throw err;
          assert.deepEqual(JSON.parse(res[0]._data[1].data.toString()), {"resources": [{"name": "b"}]});
          assert.equal(res[0]._data[0].id, '0');
          assert.equal(res[0]._data[1].id, '1');
          done();
        });
      });
    });


    it('should create a batch of task', function(done){    
      sutil.addPaths.call(squirrel, inputs, function(err, inputs){
        sutil.addIdAndStdinData.call(squirrel, inputs, function(err, res){
          step.map[0].inputs = res;

          var batch = squirrel._makeTasksMap(step.map[0]);

          assert.equal(batch[0].command, './bintestjs');
          assert.equal(batch[0].options.cwd, path.resolve(root, 'bin'));
          assert.equal(batch[0].options.timeout, 60*1000);
          assert.deepEqual(batch[0].args, [ path.resolve(root, 'data'), '0', 'X']);
          assert.deepEqual(batch[1].args, [ path.resolve(root, 'data'), '1', 'X']);
          
          done();
        });
      });
    });

    it('map should work', function(done){

      squirrel.map(dpkg.pipeline[0], function(err){
        if (err) throw err;
        
        var mydpkg = squirrel.dpkg;

        assert.deepEqual(mydpkg.resources, expectedResources.slice(0,-1));
        for(var i=1; i<mydpkg.resources.length; i++){
          assert(fs.existsSync(mydpkg.resources[i].path));
        }
        done();
      });
    });

    it('reduce should work', function(done){

      squirrel.map(dpkg.pipeline[0], function(err){
        if (err) throw err;
        squirrel.reduce(dpkg.pipeline[0], function(err){
          if (err) throw err;

          assert.deepEqual(squirrel.dpkg.resources, expectedResources);
          for(var i=1; i<squirrel.dpkg.resources.length; i++){
            assert(fs.existsSync(squirrel.dpkg.resources[i].path));
          }
          done();
        });

      });
    });

    it('start should work', function(done){
      squirrel.start(function(err, newDpkg){

        assert.deepEqual(newDpkg.resources, expectedResources);
        for(var i=1; i<newDpkg.resources.length; i++){
          assert(fs.existsSync(newDpkg.resources[i].path));
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
});
