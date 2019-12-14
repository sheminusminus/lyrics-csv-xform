const fs = require('fs');
const path = require('path');
const papa = require('papaparse');

const rowsLimit = -1;

const lyricsCsv = path.resolve(__dirname, 'data/lyrics.csv');
const billboardCsv = path.resolve(__dirname, 'data/billboard.csv');

const colsLyrics = ['Index', 'Song', 'Year', 'Artist', 'Genre', 'Lyrics'];
const colsBillboard = ['Rank', 'Song', 'Artist', 'Year', 'Lyrics'];

const outFileName = rowsLimit === -1 ? 'lyrics' : 'lyrics_example';
const fileOut = path.resolve(__dirname, 'out', `${outFileName}.csv`);
const writeStream = fs.createWriteStream(fileOut);

const LyricsCols = {
  ARTIST: colsLyrics.indexOf('Artist'),
  LYRICS: colsLyrics.indexOf('Lyrics'),
  SONG: colsLyrics.indexOf('Song'),
  YEAR: colsLyrics.indexOf('Year'),
};

const BillboardCols = {
  ARTIST: colsBillboard.indexOf('Artist'),
  LYRICS: colsBillboard.indexOf('Lyrics'),
  SONG: colsBillboard.indexOf('Song'),
  YEAR: colsBillboard.indexOf('Year'),
};

const finalCols = ['Decade', 'Year', 'Artist', 'Song', 'Lyrics'];

let filesParsed = 0;

function getDecade(yearStr) {
  const dropZero = yearStr.substring(0, 3);
  return `${dropZero}0s`;
}

function readData(filePath, cols) {
  if (filesParsed === 0) {
    const headersData = `${finalCols.join(',')}\n`;
    writeStream.write(headersData);
  }

  const readStream = fs.createReadStream(filePath);
  const parseStream = papa.parse(papa.NODE_STREAM_INPUT);
  readStream.pipe(parseStream);

  let index = 0;

  parseStream.on('data', (d) => {
    if (rowsLimit === -1 || index < rowsLimit) {
      const year = d[cols.YEAR];
      const decade = getDecade(year);
      const artist = d[cols.ARTIST];
      const lyrics = d[cols.LYRICS];
      const song = d[cols.SONG];
      const row = `${decade},${year},${JSON.stringify(artist)},${JSON.stringify(song)},${JSON.stringify(lyrics)}\n`;
      writeStream.write(row);
    }

    index += 1;
  });

  parseStream.on('end', (d) => {
    if (filesParsed === 0) {
      filesParsed += 1;
      index = 0;
      readData(lyricsCsv, LyricsCols);
    } else {
      writeStream.end();
    }
  });
}

readData(billboardCsv, BillboardCols);
