const argv = require('yargs').argv;
const base64 = require('base64-js');
const fs = require('fs');
const ipaddr = require('ipaddr.js');
const parser = require('csv-parse')();
const protocolsArr = JSON.parse(fs.readFileSync('protocols.json')).map((p) => p.toUpperCase());

const cols = {
  START_TIME: 0,
  END_TIME: 1,
  DUR: 2,
  SRC_ADDR: 3,
  DST_ADDR: 4,
  SRC_PORT: 5,
  DST_PORT: 6,
  PROTOCOL: 7,
  FLAGS: 8,
  FWD_STATUS: 9,
  SRC_TOS: 10,
  PACKETS_IN: 11,
  BYTES_IN: 12,
  PACKETS_OUT: 13,
  BYTES_OUT: 14
}

parser.on('readable', function() {
  let record;
  let rowNum = 1;
  parser.read();
  while (record = parser.read()) {
    try {
      let output = [
        record[cols.START_TIME],
        record[cols.END_TIME],
        record[cols.DUR],
        toHexString(ipaddr.parse(record[cols.SRC_ADDR]).toByteArray()),
        record[cols.SRC_PORT],
        toHexString(ipaddr.parse(record[cols.DST_ADDR]).toByteArray()),
        record[cols.DST_PORT],
        protocolsArr.indexOf(record[cols.PROTOCOL]),
        record[cols.PACKETS_IN],
        record[cols.BYTES_IN],
        record[cols.PACKETS_OUT],
        record[cols.BYTES_OUT]
      ];
      console.log(output.map((str) => csvEscape(str)).join(','));
    } catch (err) {
      console.error('failed to parse record [' + rowNum + ']', err);
    } finally {
      rowNum++;
    }
  }
});
parser.on('error', function(err) {
  console.error(err.message);
});

if (argv._.length > 0) {
  let inFile = fs.createReadStream(argv._[0]);
  inFile.pipe(parser);
} else {
  process.stdin.pipe(parser);
}


function csvEscape(str) {
  if (str && typeof str !== 'string') {
    str = String(str);
  }
  if (str && str.indexOf(',') < 0) {
    return str;
  } else if (str) {
    return '"' + str.replace('"', '\\"') + '"';
  }
  return str;
}
function toHexString(byteArray) {
  return '0x' + Array.from(byteArray, function(byte) {
    return ('0' + (byte & 0xFF).toString(16)).slice(-2);
  }).join('')
}