import RequestWorker from './requestWorker'

const LRU = require('quick-lru')

export default class Window {
  /**
   * View into a subset of the data in a BigWig file.
   *
   * Adapted by Robert Buels from bigwig.js in the Dalliance Genome
   * Explorer by Thomas Down.
   * @constructs
   */
  constructor(bwg, cirTreeOffset, cirTreeLength, isSummary, autoSql) {
    this.bwg = bwg
    this.autoSql = autoSql
    if (!(cirTreeOffset >= 0)) throw new Error('invalid cirTreeOffset!')
    if (!(cirTreeLength > 0)) throw new Error('invalid cirTreeLength!')

    this.cirTreeOffset = cirTreeOffset
    this.cirTreeLength = cirTreeLength
    this.isSummary = isSummary

    function countFeatures(features) {
      if (!features) return 0
      let total = features.length
      features.forEach(feature => {
        total += countFeatures(feature.children())
      })
      return total
    }
    this.featureCache = new LRU({
      name: 'feature cache',
      fillCallback: (query, callback) => {
        this.readWigDataById(...query, callback, err => {
          console.error(err)
        })
      },
      sizeFunction: countFeatures,
      maxSize: 500000, // cache up to 50000 features and subfeatures
    })
  }

  readWigData(chrName, min, max, callback, errorCallback) {
    // console.log( 'reading wig data from '+chrName+':'+min+'..'+max);
    const chr = this.bwg.header.refsByName[chrName]
    console.log(chr,'test')
    if (!chr) {
      callback([])
    } else {
      this.readWigDataByIdWithCache(chr.id, min, max, callback, errorCallback)
    }
  }

  readWigDataByIdWithCache(chr, min, max, callback, errorCallback) {
    this.featureCache.get([chr, min, max], (result, error) => {
      if (error) errorCallback(error)
      else callback(result)
    })
  }

  readWigDataById(chr, min, max, callback, errorCallback) {
    if (!this.cirHeader) {
      const readCallback = () => {
        this.readWigDataById(chr, min, max, callback, errorCallback)
      }
      if (this.cirHeaderLoading) {
        this.cirHeaderLoading.push(readCallback)
      } else {
        this.cirHeaderLoading = [readCallback]
        // dlog('No CIR yet, fetching');
        this.bwg.data.read(
          this.cirTreeOffset,
          48,
          result => {
            this.cirHeader = result
            this.cirBlockSize = this.bwg.newDataView(result, 4, 4).getUint32()
            this.cirHeaderLoading.forEach(c => {
              c()
            })
            delete this.cirHeaderLoading
          },
          errorCallback,
        )
      }
      return
    }

    // dlog('_readWigDataById', chr, min, max, callback);

    const worker = new RequestWorker(
      this,
      chr,
      min,
      max,
      callback,
      errorCallback,
    )
    worker.cirFobRecur([this.cirTreeOffset + 48], 1)
  }
}
