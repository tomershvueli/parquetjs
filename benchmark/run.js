const fastcsv = require('csv-parser');
const parquet = require('../parquet.js');
const zlib = require("zlib");
const fs = require("fs");
const csvParser = fastcsv({ separator : '\t', skipLines: 2, headers: ['date',"time","location","bytes","requestip","method","host","uri","status","referrer","useragent","querystring","cookie","resulttype","requestid","hostheader","requestprotocol","requestbytes","timetaken","xforwardedfor","sslprotocol","sslcipher","responseresulttype","httpversion","filestatus","encryptedfields","viewerport","ttfb","edgedetailederror","contenttype","contentlength","startrange","endrange"] });
const unzip = zlib.createUnzip();

async function csvToParquet() {
  const writer = await parquet.ParquetWriter.openFile(schema, __dirname + `/test.parquet`);
  const file_stream = fs.createReadStream(__dirname + "/test.gz");

  const start = Date.now();
  for await (const row of file_stream.pipe(unzip).pipe(csvParser)) {
    row.datetime = new Date(row.date + " " + row.time);
    row.startrange = row.startrange == "-" ? undefined : row.startrange;
    row.endrange = row.endrange == "-" ? undefined : row.endrange;
    row.encryptedfields = row.encryptedfields == "-" ? undefined : row.encryptedfields;
    row.contentlength = row.contentlength == "-" ? undefined : row.contentlength;
    row.referrer = row.referrer == "-" ? undefined : row.referrer;
    row.cookie = row.cookie == "-" ? undefined : row.cookie;
    row.xforwardedfor = row.xforwardedfor == "-" ? undefined : row.xforwardedfor;
    row.filestatus = row.filestatus == "-" ? undefined : row.filestatus;
    row.useragent = row.useragent == "-" ? undefined : decodeURI(row.useragent);
    await writer.appendRow(row);
  }
  await writer.close();
  console.log("This conversion took", (Date.now() - start)/1000, "seconds.")
}

const COMPRESSION = 'SNAPPY';

let schema = new parquet.ParquetSchema({
  datetime: { type: 'TIMESTAMP_MILLIS', optional: true, compression: COMPRESSION },
  location: { type: 'UTF8', optional: true , compression: COMPRESSION },
  bytes: { type: 'INT64', optional: true , compression: COMPRESSION },
  requestip: { type: 'UTF8', optional: true, compression: COMPRESSION  },
  method: { type: 'UTF8', optional: true , compression: COMPRESSION },
  host: { type: 'UTF8', optional: true, compression: COMPRESSION  },
  uri: { type: 'UTF8', optional: true , compression: COMPRESSION },
  status: { type: 'INT32', optional: true, compression: COMPRESSION  },
  referrer: { type: 'UTF8', optional: true , compression: COMPRESSION },
  useragent: { type: 'UTF8', optional: true , compression: COMPRESSION },
  querystring: { type: 'UTF8', optional: true , compression: COMPRESSION },
  cookie: { type: 'UTF8', optional: true , compression: COMPRESSION },
  resulttype: { type: 'UTF8', optional: true , compression: COMPRESSION },
  requestid: { type: 'UTF8', optional: true, compression: COMPRESSION  },
  hostheader: { type: 'UTF8', optional: true , compression: COMPRESSION },
  requestprotocol: { type: 'UTF8', optional: true , compression: COMPRESSION },
  requestbytes: { type: 'INT64', optional: true , compression: COMPRESSION },
  timetaken: { type: 'FLOAT', optional: true , compression: COMPRESSION },
  xforwardedfor: { type: 'UTF8', optional: true , compression: COMPRESSION },
  sslprotocol: { type: 'UTF8', optional: true , compression: COMPRESSION },
  sslcipher: { type: 'UTF8', optional: true , compression: COMPRESSION },
  responseresulttype: { type: 'UTF8', optional: true, compression: COMPRESSION  },
  httpversion: { type: 'UTF8', optional: true, compression: COMPRESSION  },
  filestatus: { type: 'UTF8', optional: true , compression: COMPRESSION },
  encryptedfields: { type: 'INT32', optional: true , compression: COMPRESSION },
  viewerport: { type: 'INT32', optional: true , compression: COMPRESSION },
  ttfb: { type: 'FLOAT', optional: true, compression: COMPRESSION  },
  edgedetailederror: { type: 'UTF8', optional: true , compression: COMPRESSION },
  contenttype: { type: 'UTF8', optional: true, compression: COMPRESSION  },
  contentlength: { type: 'INT64', optional: true , compression: COMPRESSION },
  startrange: { type: 'INT64', optional: true , compression: COMPRESSION },
  endrange: { type: 'INT64', optional: true , compression: COMPRESSION }
});

csvToParquet();
