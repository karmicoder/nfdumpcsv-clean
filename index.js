const argv = require('yargs')
  .alias('f', 'file')
  .boolean('noOutput')
  .argv;
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
        binToStr(ipToBin(record[cols.SRC_ADDR])),
        record[cols.SRC_PORT],
        binToStr(ipToBin(record[cols.DST_ADDR])),
        record[cols.DST_PORT],
        Math.max(0, protocolsArr.indexOf(record[cols.PROTOCOL])),
        record[cols.PACKETS_IN],
        record[cols.BYTES_IN],
        record[cols.PACKETS_OUT],
        record[cols.BYTES_OUT]
      ];
      if (!argv.noOutput) {
        console.log(output.map((str) => csvEscape(str)).join(','));
      }
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

if (argv.f) {
  let inFile = fs.createReadStream(argv.f);
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
function binToStr(byteArray) {
  return '[' + byteArray.join(',') + ']';
}

function ipToBin(ip) {
  let addr = ipaddr.parse(ip);
  switch (addr.kind()) {
    case 'ipv4':
      return new Array(12).fill(255, 0, 12)
        .concat(addr.toByteArray());
      break;
    case 'ipv6':
      return addr.toByteArray();
      break;
    default:
      throw new Error("Unrecognized ip address kind for " + ip + ": " + addr.kind());
  }
}