//var async = require('async-arrays');

var Adapter = function(options){
    this.options = options || {};
    this.client = options.client || this.client(options.pool);
    this.connect(()=>{});
}

// IMPL SPECIFIC
var abstract = function(cls, fnName){ cls.prototype[fnName] = function(){
    throw new Error( '.'+fnName+'() was not implemented');
}};

[
    'connect', 'cleanup', 'loaderSQL', 'loaderResultsHandler', 'existsSQL',
    'existsResultsHandler', 'sqlTypeFromJavascriptType', 'createSQL',
    'createResultsHandler', 'saveSQL', 'saveResultsHandler', 'client'
].forEach((fnName)=>{
    abstract(Adapter, fnName);
});
// END IMPL SPECIFIC

Adapter.prototype.load = function(name, options, handler, cb){
    var query = this.loaderSQL(name);
    //todo: handle query option
    this.connection.then(()=>{
        this.engine.query(query, (err, res)=>{
            this.loaderResultsHandler(err, res, handler, cb);
        });
    });
}

Adapter.prototype.exists = function(name, options, cb){
    //todo: optional existence record
    //todo: optimized mode short circuit (errs on nonexistence )
    var query = this.existsSQL(name);
    this.connection.then(()=>{
        this.engine.query(query, (err, res)=>{
            this.existsResultsHandler(err, res, null, (resultsErr, exists)=>{
                if(!exists){
                    if(!options.object) return cb(new Error(
                        'Table does not exist, and an example object is needed to generate one!'
                    ));
                    var createQuery = this.createSQL(name, options.object, options.primaryKey);
                    this.engine.query(createQuery, (createErr, createRes)=>{
                        if(createErr) return cb(createErr);
                        cb(null, {created: true});
                    });
                }else{
                    cb(null, {});
                }
            });
        });
    });
}

Adapter.prototype.loadCollection = function(collection, name, options, cb){
    this.load(name, options, function(item){
      collection.index[item[collection.primaryKey]] = item;
    }, cb);
}

Adapter.prototype.saveCollection = function(collection, name, options, cb){
    var lcv = 0;
    var list;
    this.save(name, options, collection, function(){
        if(!list) list = Object.keys(collection.index);
        return list.length?collection.index[list.shift()]:null;
    }, cb);
}

Adapter.prototype.save = function(name, options, collection, next, cb){
    //loop through batches and save a chunk of ids as
    var writtenOne = false;
    var writeChain = ()=>{
        //todo: make chains batchable to a requestable size
        var item = next();
        if(item){
            var fields = Object.keys(item);
            var values;
            var query = this.saveSQL(name, fields, collection.primaryKey, item, (vs)=>{
                values = vs;
            });
            this.engine.query(query, values, (writeErr, res)=>{
                this.saveResultsHandler(writeErr, res, null, (saveErr)=>{
                    if(!writtenOne) writtenOne = true;
                    if(saveErr) return cb(saveErr);
                    writeChain();
                });
            });
        }else{
            cb();
        }
    }
    this.exists(name, {}, ()=>{
        writeChain();
    });
}

Adapter.prototype.query = function(q, cb){
    //allows symbolic saving to be executed remotely (instead of as a set)

}

var makeMergedCopyAndExtendify = function(ext, supr, cls){
    var copy = supr || function(){};
    //var copy = function(){ return orig.apply(this, arguments) };
    Object.keys(cls.prototype).forEach(function(key){
        copy.prototype[key] = cls.prototype[key];
    });
    Object.keys(ext).forEach(function(key){
        copy.prototype[key] = ext[key];
    });
    copy.extend = function(ext, supr){
        return makeMergedCopyAndExtendify(ext, supr, copy);
    };
    return copy;
}


Adapter.extend = function(cls, cns){
    var cons = cns || function(){
        Adapter.apply(this, arguments);
    };
    return makeMergedCopyAndExtendify(cls, cons, Adapter);
};

module.exports = Adapter;
