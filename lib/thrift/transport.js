/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

var util = require('util');
var noop = function() {};

var VAL32 = 0x100000000;
function readInt64BEasNumber(buf, offset) {
	return buf.readInt32BE(offset, true) * VAL32 + buf.readUInt32BE(offset + 4, true);
}

function writeNumberasInt64BE(buf, offset, v) {
	var negate = v < 0 ? -1 : 1;
	v = Math.abs(v);
	var lo = v % VAL32;
	var hi = (v / VAL32) * negate;
	if (hi > VAL32) throw new RangeError('Attempt to convert number outside Int64 range');
	buf.writeInt32BE(hi, offset, true);
	buf.writeUInt32BE(lo, offset + 4, true);
}

var InputBufferUnderrunError = exports.InputBufferUnderrunError = function() {
};

var TTransport = exports.TTransport = function(buf, onFlush) {
	this.reset(buf, onFlush);
};

TTransport.receiver = function(callback) {
	return function(data) {
		callback(new TTransport(data));
	};
};

TTransport.prototype = {
		reset: function(buf, onFlush) {
			this.pos = 0;
			if(buf) {
				this.buf = buf;
				if(onFlush)
					this.onFlush = onFlush;
			}
		},

		skip: function(len) {
			this.pos += len;
		},

		read: function(len) {
			var end = this.pos + len;
			if (this.buf.length < end)
				throw new InputBufferUnderrunError('read(' + len + ') failed - not enough data');

			var buf = this.buf.slice(this.pos, end);
			this.pos = end;
			return buf;
		},

		readByte: function() {
			var buf = this.buf, pos = this.pos, end = pos + 1;
			if (buf.length < end)
				throw new InputBufferUnderrunError('readByte() failed - not enough data');
			var b = buf.readInt8(pos, true);
			this.pos = end;
			return b;
		},

		readI16: function() {
			var buf = this.buf, pos = this.pos, end = pos + 2;
			if (buf.length < end)
				throw new InputBufferUnderrunError('readI16() failed - not enough data');
			var i16 = buf.readInt16BE(pos, true);
			this.pos = end;
			return i16;
		},

		readI32: function() {
			var buf = this.buf, pos = this.pos, end = pos + 4;
			if (buf.length < end)
				throw new InputBufferUnderrunError('readI32() failed - not enough data');
			var i32 = buf.readInt32BE(pos, true);
			this.pos = end;
			return i32;
		},

		readI64: function() {
			var buf = this.buf, pos = this.pos, end = pos + 8;
			if (buf.length < end)
				throw new InputBufferUnderrunError('readI64() failed - not enough data');
			var i64 = readInt64BEasNumber(buf, pos)
			this.pos = end;
			return i64;
		},

		readDouble: function() {
			var buf = this.buf, pos = this.pos, end = pos + 8;
			if (buf.length < end)
				throw new InputBufferUnderrunError('readDouble() failed - not enough data');
			var d = buf.readDoubleBE(pos, true);
			this.pos = end;
			return d;
		},

		readString: function(len) {
			var buf = this.buf, pos = this.pos, end = pos + len;
			var str = buf.toString('utf8', pos, end);
			this.pos = end;
			return str;
		},

		readAll: function() {
			return this.buf.slice(0, this.pos);
		},

		writeByte: function(v) {
			var buf = this.buf, pos = this.pos, end = pos + 1;
			if (buf.length < end)
				throw new InputBufferUnderrunError('writeByte() failed - not enough space');
			buf.writeInt8(v, pos, true);
			this.pos = end;
		},

		writeI16: function(v) {
			var buf = this.buf, pos = this.pos, end = pos + 2;
			if (buf.length < end)
				throw new InputBufferUnderrunError('writeI16() failed - not enough space');
			buf.writeInt16BE(v, pos, true);
			this.pos = end;
		},

		writeI32: function(v) {
			var buf = this.buf, pos = this.pos, end = pos + 4;
			if (buf.length < end)
				throw new InputBufferUnderrunError('writeI32() failed - not enough space');
			buf.writeInt32BE(v, pos, true);
			this.pos = end;
		},

		writeI64: function(v) {
			var buf = this.buf, pos = this.pos, end = pos + 8;
			if (buf.length < end)
				throw new InputBufferUnderrunError('writeI64() failed - not enough space');
			writeNumberasInt64BE(buf, pos, v);
			this.pos = end;
		},

		writeDouble: function(v) {
			var buf = this.buf, pos = this.pos, end = pos + 8;
			if (buf.length < end)
				throw new InputBufferUnderrunError('writeDouble() failed - not enough space');
			buf.writeDoubleBE(v, pos, true);
			this.pos = end;
		},

		write: function(v) {
			var buf = this.buf, pos = this.pos, end;
			if (typeof(v) === 'string') {
				end = pos + Buffer.byteLength(v);
				if (buf.length < end)
					throw new InputBufferUnderrunError('write() failed - not enough space');
				buf.write(v, pos);
			} else {
				end = pos + v.length;
				if (buf.length < end)
					throw new InputBufferUnderrunError('write() failed - not enough space');
				v.copy(buf, pos);
			}
			this.pos = end;
		},

		writeWithLength: function(v) {
			var buf = this.buf, pos = this.pos, bufPos = pos + 4, len, end;
			var bufPos = pos + 4;
			if (typeof(v) === 'string') {
				len = Buffer.byteLength(v);
				end = bufPos + len;
				if (buf.length < end)
					throw new InputBufferUnderrunError('write() failed - not enough space');
				buf.write(v, bufPos);
			} else {
				len = v.length;
				end = bufPos + len;
				if (buf.length < end)
					throw new InputBufferUnderrunError('write() failed - not enough space');
				v.copy(buf, bufPos);
			}
			buf.writeInt32BE(len, pos, true);
			this.pos = end;
		},

		flush: function(flushCallback) {
			flushCallback = flushCallback || this.onFlush;
			if(flushCallback) {
				var pos = this.pos, buf = new Buffer(pos);
				this.buf.copy(buf, 0, 0, pos);
				flushCallback(buf);
			}
		}
};
