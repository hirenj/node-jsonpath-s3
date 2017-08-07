'use strict';

const helpers = require('lambda-helpers');

const AWS = helpers.AWS;
const s3 = new AWS.S3();
const JSONStream = require('JSONStream');
const fs = require('fs');

const Transform = require('stream').Transform;
const inherits = require('util').inherits;


function MetadataExtractor(options) {
  if ( ! (this instanceof MetadataExtractor))
    return new MetadataExtractor(options);

  if (! options) options = {};
  options.objectMode = true;
  let self = this;
  let Parser = require('JSONStream/node_modules/jsonparse');
  let p = new Parser();

  this.parser = p;

  p.push = function(){
    if (this.stack && this.stack[1] && this.stack[1].key == 'data') {
      this.value = null;
    }
    this.stack.push({value: this.value, key: this.key, mode: this.mode});
  };

  p.onValue = function(val) {
    if (! val) {
      return;
    }
    if (val.metadata) {
      self.metadata = val.metadata;
      self.push(val.metadata);
    }
  };

  Transform.call(this, options);
}

inherits(MetadataExtractor, Transform);

MetadataExtractor.prototype._transform = function _transform(obj, encoding, callback) {
  this.parser.write(obj);
  callback();
};

function Offsetter(offset,options) {
  if ( ! (this instanceof Offsetter))
    return new Offsetter(offset,options);

  if (! options) options = {};
  options.objectMode = true;
  this.startOffset = offset;
  if ( ! this.startOffset ) {
    this.startOffset = 0;
    this.done = true;
  }
  Transform.call(this, options);
}

inherits(Offsetter, Transform);

Offsetter.prototype._transform = function _transform(obj, encoding, callback) {
  this.offset = this.startOffset + (this.bytesConsumed || 0) - 1024*1024;
  this.bytesConsumed = (this.bytesConsumed || 0) + obj.length;
  if (this.done) {
    this.push(obj);
    callback();
    return;
  }

  let chunk = obj.toString();
  let newline = chunk.indexOf('\n');

  if (newline < 0) {
    callback();
    return;
  }
  this.done = true;
  this.push('{ "data" : {'+chunk.substring(newline));
  callback();
};

const parse_path = function parse_path(s3path) {
  if (typeof s3path !== 'string' && s3path.Key && s3path.Bucket) {
    return s3path;
  }
  s3path = s3path.replace('s3://','');
  let bits = s3path.split('/');
  return {
    'Bucket' : bits[0],
    'Key' : bits.slice(1).join('/')
  };
};

const stat_value = function(filename) {
  let stats = fs.statSync(filename);
  return stats.size;
};

const retrieve_file_local = function retrieve_file_local(filekey,offset) {
  let path = filekey.replace('file://','')
  let size = stat_value(path);
  if ( ! offset || (offset >= 0)) {
    offset = -1*size;
  }
  if ( Math.abs(offset) > size ) {
    offset = -1*size;
  }
  return Promise.resolve(fs.createReadStream(path, { start: (size + offset) }));
};

const retrieve_file_s3 = function retrieve_file_s3(s3path,byte_offset) {
  if (typeof s3path == 'string' && s3path.indexOf('file://') == 0) {
    return retrieve_file_local(s3path,byte_offset);
  }
  let params = parse_path(s3path);
  return s3.headObject(params).promise().then( head => {
    if (head.ContentLength < Math.abs(byte_offset)) {
      byte_offset = byte_offset < 1 ? 0 : head.ContentLength;
    }
  }).then( () => {
    if (byte_offset) {
      params.Range = byte_offset > 0 ? `bytes=${byte_offset}-` : `bytes=${byte_offset}`;
    }
    let request = s3.getObject(params);
    let stream = request.createReadStream();
    if (! byte_offset) {
      stream.start = 0;
    }
    return stream;
  });
};

const stream_end_promise = function(stream) {
  return new Promise( (resolve,reject) => {
    stream.on('end', resolve );
    stream.on('finish', resolve );
    stream.on('error', reject );
  });
};

const get_data_stream = function(s3path) {
  let stream_promise = retrieve_file_s3(s3path);
  let json_parser = JSONStream.parse(['data', {'emitKey': true}]);
  return stream_promise.then( stream => {
    let output = stream.pipe(json_parser);
    output.finished = stream_end_promise(stream);
    return output;
  });
};

const get_metadata_stream = function(s3path,offset) {
  let stream_promise = retrieve_file_s3(s3path,offset || -50*1024);
  return stream_promise.then( stream => {
    let output = stream.pipe(new Offsetter(stream.start === 0 ? 0 : 1)).pipe(new MetadataExtractor());
    output.finished = stream_end_promise(stream);
    return output;
  });
};

exports.get_data_stream = get_data_stream;
exports.get_metadata_stream = get_metadata_stream;
