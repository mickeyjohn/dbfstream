const fs = require('fs');
const iconv = require('iconv-lite');
const util = require('util');
const EventEmitter = require('events').EventEmitter;
const Readable = require('stream').Readable;
const isStream = require('is-stream');

const fileTypes = {
  2: 'FoxBASE',
  3: 'FoxBASE+/Dbase III plus, no memo',
  48: 'Visual FoxPro',
  49: 'Visual FoxPro, autoincrement enabled',
  50: 'Visual FoxPro with field type Varchar or Varbinary',
  67: 'dBASE IV SQL table files, no memo',
  99: 'dBASE IV SQL system files, no memo',
  131: 'FoxBASE+/dBASE III PLUS, with memo',
  139: 'dBASE IV with memo',
  203: 'dBASE IV SQL table files, with memo',
  245: 'FoxPro 2.x (or earlier) with memo',
  229: 'HiPer-Six format with SMT memo file',
  251: 'FoxBASE',
};

const HEADER = 0;
const FIELDS = 1;
const RECORDS = 2;
const READ_BLOCKED = 99;

const parseFileType = (buffer) => fileTypes[buffer.readUInt8(0, true)]
  ? fileTypes[buffer.readUInt8(0, true)]
  : 'uknown';

const parseDate = (buffer) => new Date(
  buffer.readUInt8(0, true) + 1900, // year
  buffer.readUInt8(1, true) - 1,  // month
  buffer.readUInt8(2, true) // date
);

// 12 – 27: Reserved
// 28: Table flags
// 29: Code page mark
// 30 - 31: Reserved, contains 0x00
const getHeader = (readStream) => {
  const buffer = readStream.read(32);
  return buffer ? {
    type: parseFileType(buffer),
    dateUpdated: parseDate(buffer.slice(1, 4)),
    numberOfRecords: buffer.readInt32LE(4, true),
    bytesOfHeader: buffer.readInt16LE(8, true),
    lengthPerRecord: buffer.readInt16LE(10, true),
  } : undefined;
};

// 19 - 22	Value of autoincrement Next value
// 23	Value of autoincrement Step value
// 24 – 31	Reserved
const getField = (buffer) => (
  buffer.length < 32
    ? undefined
    : {
      name: buffer.toString('utf-8', 0, 11).replace(/[\u0000]+$/, ''),
      type: buffer.toString('utf-8', 11, 12),
      displacement: buffer.readInt32LE(12, true),
      length: buffer.readUInt8(16, true),
      decimalPlaces: buffer.readUInt8(17, true),
      flag: buffer.readUInt8(18, true),
    }
);

const getListOfFields = (readStream, bytesOfHeader) => {
  const buffer = readStream.read(bytesOfHeader - 32);

  if (!buffer) return null;

  const listOfFields = [];
  for (let i = 0, len = buffer.length; i < len; i += 32) {
    let field;
    if (field = getField(buffer.slice(i, i + 32))) {
      listOfFields.push(field);
    }
  }
  return listOfFields;
};

const dataTypes = {
  C(data) {
    return data;
  },
  N(data) {
    return +data;
  },
  L(data) {
    return data.toLowerCase() === 't';
  },
};

const parseDataByType = (data, type) => (
  dataTypes[type]
    ? dataTypes[type](data)
    : data  // default
);

const convertToObject = (data, listOfFields, encoding, numOfRecord) => {
  const row = {
    '@numOfRecord': numOfRecord,
    '@deleted': data.slice(0, 1)[0] !== 32,
  };

  listOfFields.reduce(function (acc, now) {
    const value = iconv
      .decode(data.slice(acc, acc + now.length), encoding)
      .replace(/^\s+|\s+$/g, '');
    row[now.name] = parseDataByType(value, now.type);
    return acc + now.length;
  }, 1);

  return row;
};

/**
 * DBF Stream is for reading a dbf file or stream
 * @param {*} source  source file or stream
 * @param {*} encoding endcoding, default utf-8
 */
const dbfStream = (source, encoding = 'utf-8') => {
  util.inherits(Readable, EventEmitter);
  const stream = new Readable({ objectMode: true });
  const _isStream = isStream.readable(source);
  // If source is already a readableStream, use it, otherwise treat as a filename
  const readStream = _isStream ? source : fs.createReadStream(source);
  let numOfRecord = 1;   //row number numOfRecord

  _invalidTry = 0;
  _readOn = false;
  _readState = READ_BLOCKED;

  const _onData = () => {
    let readRes = true;
    try {
      // Loop while there was still data to process on the stream's internal buffer. 
      // This will stop when we don't have enough readable data or encountering a back pressure issue;      
      do {
        switch (_readState) {
          case HEADER:
            stream.header = readRes = getHeader(readStream);
            if (readRes) {
              if (!stream.header.numberOfRecords || !stream.header.lengthPerRecord)
                throw new Error('Empty dbf file');
              // Check for physical size and header consistency
              if (!_isStream && _size !== (stream.header.bytesOfHeader + (stream.header.numberOfRecords * stream.header.lengthPerRecord)))
                throw new Error('Invalid dbf file');

              _readState = FIELDS;
            } else {
              _checkReadTry();
            }
            break;
          case FIELDS:
            readRes = getListOfFields(readStream, stream.header.bytesOfHeader);
            if (readRes) {
              stream.header.listOfFields = readRes;

              if (!stream.header.listOfFields.length)
                throw new Error('No field dbf file');

              stream.emit('header', stream.header);
              _readState = RECORDS;
            } else {
              _checkReadTry();
            }
            break;
          case RECORDS:
            readRes = readStream.read(stream.header.lengthPerRecord);
            if (readRes) {
              // Push the message onto the read buffer for the consumer to read. We are mindful of slow reads by the consumer 
              // and will respect backpressure signals.
              if (stream.push(convertToObject(readRes, stream.header.listOfFields, encoding, numOfRecord++))) {
                _readState = RECORDS;
              } else {
                _readState = READ_BLOCKED;
                readRes = false;
              }
            }
            break;
          case READ_BLOCKED:
            readRes = null;
            break;
          default:
            throw new Error('Unknown read state');
        }
      } while (readRes);
    } catch (err) {
      _errorHandler(err);
    }
  };

  const _checkReadTry = () => {
    _invalidTry++;
    if (_invalidTry >= 3)
      throw new Error('Corrupted or not a dbf file !');
  }

  const _errorHandler = (err) => {
    if (readStream) {
      readStream.pause();
      readStream.emit('end');
    }
    if (stream) {
      stream.emit('error', err);
      stream.push(null);
    }
  }

  readStream._maxListeners = Infinity;

  const _onDataFunc = () => setImmediate(() => _onData());

  readStream.once('end', () => {
    readStream.removeListener('readable', _onDataFunc);
    stream.push(null);
  });

  stream._read = () => {
    if (!_readOn) {
      readStream.on('readable', _onDataFunc);
      _readOn = true;
    }
  };

  if (!_isStream) {
    fs.stat(source, (err, stat) => {
      if (err) return _errorHandler(err);
      _size = stat.size;
      _readState = HEADER;
    });
  }
  else {
    _readState = HEADER;
  }

  return stream;
};

module.exports = dbfStream;
