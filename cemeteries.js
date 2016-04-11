const fs = require('fs')
const path = require('path')
const H = require('highland')
const slurper = require('spreadsheet-slurper')
const parse = require('wellknown')

const CEMETERIES_SHEET_KEY = '1yiIaR24sJnhOO_QOJA-4KggcBhTNPGWp8atkkiveKTU'

function download (config, dirs, tools, callback) {
  return slurper.slurp(CEMETERIES_SHEET_KEY)
    .map(JSON.stringify)
    .intersperse('\n')
    .append('\n')
    .on('end', callback)
    .pipe(fs.createWriteStream(path.join(dirs.current, 'cemeteries.ndjson')))
}

function transform (config, dirs, tools, callback) {
  H(fs.createReadStream(path.join(dirs.previous, 'cemeteries.ndjson')))
    .split()
    .compact()
    .map(JSON.parse)
    .map((row) => {
      var pit = {
        id: row.id,
        name: row.name,
        type: 'st:Cemetery',
        geometry: row.geometry ? parse(row.geometry) : parse(`POINT (${row.longitude} ${row.latitude})`)
      }

      if (row.yearfrom) {
        // TODO: try/catch
        pit.validSince = parseInt(row.yearfrom)
      }

      if (row.yearto) {
        // TODO: try/catch
        pit.validUntil = parseInt(row.yearto)
      }

      if (row.history || row.history || pit.records) {
        pit.data = {}
      }

      if (row.history) {
        pit.data.history = row.history
      }

      if (row.history) {
        pit.data.url = row.url
      }

      if (pit.records) {
        pit.data.records = row.records
      }

      return pit
    })
    .map((pit) => ({
      type: 'pit',
      obj: pit
    }))
    // .compact()
    // .map((s) => {
    //   console.log(JSON.stringify(s, null, 2))
    //   return null
    // })
    .compact()
    .map(H.curry(tools.writer.writeObject))
    .nfcall([])
    .series()
    .errors(callback)
    .done(callback)
}

// ==================================== API ====================================

module.exports.steps = [
  download,
  transform
]
