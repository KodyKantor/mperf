#!/usr/bin/env node
/*
 * manta-mperf: simulate Manta workloads
 */

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_getopt = require('posix-getopt');
var mod_manta = require('manta');
var mod_vasync = require('vasync');

var mod_uuid = require('uuid/v1');

var mod_fs = require('fs');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_util = require('util');

var basename = mod_path.basename(process.argv[1]);

function main() {
	var parser, option, opts;

	opts = {};

	parser = new mod_getopt.BasicParser('s:(size)d:(parent-dir)'
	    + 'q:(max-queue)i:(interval)h(help)', process.argv);
	while ((option = parser.getopt()) !== undefined) {
		switch (option.option) {
		case 's':
			opts.mp_size = parseInt(option.optarg, 10);
			if (isNaN(opts.mp_size) || opts.mp_size <= 0) {
				usage('unsupported object size. Must be between'
				    + ' 1 and 5120');
			}
			break;
		case 'd':
			opts.mp_parent_dir = option.optarg;
			break;
		case 'q':
			opts.mp_max_outstanding = parseInt(option.optarg, 10);
			if (isNaN(opts.mp_max_outstanding) ||
			    opts.mp_max_outstanding <= 0 ||
			    opts.mp_max_outstanding > 500) {
				usage('outstanding requests must be between 1'
				    + ' and 500');
			}
			break;
		case 'i':
			opts.mp_interval = parseInt(option.optarg, 10);
			if (isNaN(opts.mp_interval) ||
			    opts.mp_interval < 100 ||
			    opts.mp_interval > 10000) {
				usage('req interval must be between 100 and'
				+ ' 10000');
			}
			break;
		case 'h':
			usage();
			break;
		default:
			mod_assert.equal('?', option.option);
			usage();
			break;
		}
	}

	opts.mp_parent_dir = opts.mp_parent_dir || '~~/stor/mperf';
	opts.mp_size = opts.mp_size || 5;
	opts.mp_max_outstanding = opts.mp_max_outstanding || 20;
	opts.mp_interval = opts.mp_interval || 500;

	opts.mp_log = new mod_bunyan({
		'name': 'mperf',
		'level': process.env['LOG_LEVEL'] || 'debug',
		'stream': process.stdout
	});

	opts.mp_manta = mod_manta.createBinClient({ 'log': opts.mp_log });

	var perf = new Perf(opts);

	perf.init(function (err) {
		if (err) {
			log.error('could not initialize', err);
			process.exit(1);
		}
		setInterval(function () {
			perf.upload(function (err1) {
				if (err1) {
					opts.mp_log.error(err1);
				}
			});
		}, opts.mp_interval);
	});

}

function usage(msg) {
	if (msg) {
		console.error(basename, msg);
	}

	console.error([
		mod_util.format('usage: %s -d parent_dir [-s | --size]',
		    basename),
		'',
		'Start stress testing Manta by uploading files at a high rate.',
		'',
		'  -d, --parent-dir	parent directory target for files',
		'  -s, --size		size of uploaded files',
		'  -q, --max-queue	max number of requests queued',
		'  -i, --interval	time to wait between requests',
		'  -h, --help		print this message'
	].join('\n'));
	process.exit(1);
}

function Perf(opts) {
	mod_assert.object(opts, 'opts');
	mod_assert.object(opts.mp_log, 'opts.mp_log');
	mod_assert.object(opts.mp_manta, 'opts.mp_manta');
	mod_assert.number(opts.mp_size, 'opts.mp_size');
	mod_assert.string(opts.mp_parent_dir, 'opts.mp_parent_dir');
	mod_assert.number(opts.mp_max_outstanding, 'opts.mp_max_outstanding');

	this.p_log = opts.mp_log;
	this.p_manta = opts.mp_manta;
	this.p_size_bytes = opts.mp_size * 1024 * 1024;
	this.p_parent_dir = opts.mp_parent_dir;
	this.p_max_outstanding = opts.mp_max_outstanding;

	this.p_dir_dir = mod_path.join(this.p_parent_dir, mod_uuid());
}

Perf.prototype.init = function init(cb) {
	mod_assert.func(cb, 'cb');
	this.p_manta.mkdirp(this.p_dir_dir, cb);
};

Perf.prototype.upload = function upload(cb) {
	mod_assert.func(cb, 'cb');
	if (this.p_num_outstanding >= this.p_max_outstanding) {
		this.p_log.info('rate limiting put');
		cb();
		return;
	}

	this.p_num_outstanding++;
	function done() {
		self.p_num_outstanding--;
		cb();
		return;
	}

	var self = this;
	var log = this.p_log;
	var obj_name = mod_uuid();
	var obj_dir = mod_path.join(self.p_dir_dir, obj_name.substr(0, 2));
	var obj_path = mod_path.join(obj_dir, obj_name);

	/* options and headers sent with the Manta upload */
	var upload_opts = {
		'size': this.p_size_bytes,
		'type': 'text/plain'
	};

	var readstream = new ZeroStream({
		'size': this.p_size_bytes
	});

	var w = this.p_manta.createWriteStream(obj_path, upload_opts);
	readstream.pipe(w);
	w.once('close', done);

	w.on('error', function (err) {

		readstream.unpipe(w);
		if (err.name === 'DirectoryDoesNotExistError') {
			self.p_manta.mkdir(obj_dir, function (err1) {
				if (err1) {
					log.warn('mkdir fail', err1);
					cb();
					return;
				}
				w = self.p_manta.createWriteStream(obj_path,
				    upload_opts);
				readstream.pipe(w);
				w.once('close', done);
				w.on('error', function (err2) {
					log.warn('put fail', err2);
				});
			});

		} else {
			log.warn('put fail', err);
		}
	});
};

function ZeroStream(args) {
	mod_assert.number(args.size, 'args.size');
	this.to_stream = args.size;
	mod_stream.Readable.call(this, args);
}
mod_util.inherits(ZeroStream, mod_stream.Readable);

ZeroStream.prototype._read = function read(size) {
	var buf = Buffer.alloc(size);
	while (this.to_stream > size) {
		this.push(buf);
		this.to_stream -= size;
	}
	this.push(buf.slice(0, this.to_stream));
	this.push(null);
};


main();
