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
        type: 'st:Cemetery'
      }

      if (row.name) {
        pit.name = row.name
      }

      if (row.geometry || (row.longitude && row.latitude)) {
        pit.geometry = row.geometry ? parse(row.geometry) : parse(`POINT (${row.longitude} ${row.latitude})`)
      }

      if (row.yearfrom) {
        try {
          pit.validSince = parseInt(row.yearfrom)
        } catch (err) {
          // Do nothing, this row will be logged anyway
        }
      }

      if (row.yearto) {
        try {
          pit.validUntil = parseInt(row.yearto)
        } catch (err) {
          // Do nothing, this row will be logged anyway
        }
      }

      var log = {
        id: row.id,
        name: pit.name,
        hasName: pit.name ? true : false,
        hasGeometry: pit.geometry ? true : false,
        hasValidSince: pit.validSince ? true : false,
        hasValidUntil: pit.validUntil ? true : false
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

      var result = [
        {
          type: 'pit',
          obj: pit
        }
      ]

      // Only log if there are missing columns
      if (!log.hasName || !log.hasGeometry || (!log.hasValidSince && !log.validUntil)) {
        result.push({
          type: 'log',
          obj: log
        })
      }

      return result
    })
    .flatten()
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
