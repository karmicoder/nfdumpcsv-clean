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
      let rowBuf = Buffer.alloc(73);
      rowBuf.writeUInt32LE(Math.round(new Date(record[cols.START_TIME]).getTime() / 1000), 0);
      rowBuf.writeUInt32LE(Math.round(new Date(record[cols.END_TIME]).getTime() / 1000), 4);
      rowBuf.writeUInt32LE(Math.round(record[cols.DUR] * 1000), 8);
      ipToBin(record[cols.SRC_ADDR]).copy(rowBuf, 12, 0, 16);
      rowBuf.writeUInt16LE(Number(record[cols.SRC_PORT]), 28);
      rowBuf.write(ipToBin(record[cols.DST_ADDR]).toString('latin1'), 30, 16);
      rowBuf.writeUInt16LE(Number(record[cols.DST_PRORT]), 46);
      rowBuf.writeUInt8(Math.max(0, protocolsArr.indexOf(record[cols.PROTOCOL])), 48);
      rowBuf.writeUInt32LE(Number(record[cols.PACKETS_IN]), 49);
      rowBuf.writeUIntLE(Number(record[cols.BYTES_IN]), 53, 6);
      rowBuf.writeUInt16LE(0, 59); // pad previous to 64-bit int
      rowBuf.writeUInt32LE(Number(record[cols.PACKETS_OUT]), 61);
      rowBuf.writeUIntLE(Number(record[cols.BYTES_OUT]), 65, 6);
      rowBuf.writeUInt16LE(0, 71); // pad previous to 64-bit int

      if (!argv.noOutput) {
        process.stdout.write(rowBuf);
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

function ipToBin(ip) {
  let addr = ipaddr.parse(ip);
  let ipBytes = addr.toByteArray();
  switch (addr.kind()) {
    case 'ipv4':
      let buf = Buffer.alloc(16, 0x0);
      buf.writeUInt16LE(0xffff, 10);
      buf.writeUInt8(ipBytes[0], 12);
      buf.writeUInt8(ipBytes[1], 13);
      buf.writeUInt8(ipBytes[2], 14);
      buf.writeUInt8(ipBytes[3], 15);
      return buf;
      break;
    case 'ipv6':
      return Buffer.from(new Uint8Array(addr.toByteArray()));
      break;
    default:
      throw new Error("Unrecognized ip address kind for " + ip + ": " + addr.kind());
  }
}