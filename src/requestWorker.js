/* eslint no-bitwise: ["error", { "allow": ["|"] }] */

import Range from './range'
import { filterBlocks } from './util'

const { Parser } = require('@gmod/binary-parser')
const zlib = require('zlib')

const BIG_WIG_TYPE_GRAPH = 1
const BIG_WIG_TYPE_VSTEP = 2
const BIG_WIG_TYPE_FSTEP = 3

export default class RequestWorker {
  /**
   * Worker object for reading data from a bigwig or bigbed file.
   * Manages the state necessary for traversing the index trees and
   * so forth.
   *
   * Adapted by Robert Buels from bigwig.js in the Dalliance Genome
   * Explorer by Thomas Down.
   * @constructs
   */
  constructor(win, chr, min, max) {
    this.window = win
    this.source = win.bwg.name || undefined
    this.le = this.window.bwg.isBigEndian ? 'big' : 'little'

    this.blocksToFetch = []
    this.outstanding = 0

    this.chr = chr
    this.min = min
    this.max = max
  }

  cirFobRecur(offset, level) {
    this.outstanding += offset.length

    const maxCirBlockSpan = 4 + this.window.cirBlockSize * 32 // Upper bound on size, based on a completely full leaf node.
    let spans
    for (let i = 0; i < offset.length; i += 1) {
      const blockSpan = new Range(offset[i], offset[i] + maxCirBlockSpan)
      spans = spans ? spans.union(blockSpan) : blockSpan
    }

    const temp = spans.getRanges()
    return temp.map(fr => this.cirFobStartFetch(offset, fr, level)).flat()
  }

  cirFobStartFetch(offset, fr, level) {
    const length = fr.max() - fr.min()
    return new Promise((resolve, reject) => {
      const resultBuffer = Buffer.alloc(length)
      this.window.bwg.bbi.read(resultBuffer, 0, length, fr.min()).then(() => {
        for (let i = 0; i < offset.length; i += 1) {
          if (fr.contains(offset[i])) {
            this.cirFobRecur2(resultBuffer, offset[i] - fr.min(), level)
            this.outstanding -= 1
            if (this.outstanding === 0) {
              resolve(this.cirCompleted())
            }
          }
        }
        if (this.outstanding !== 0) {
          reject(new Error('did not complete'))
        }
      })
    })
  }

  cirFobRecur2(cirBlockData, offset, level) {
    const data = cirBlockData.slice(offset)

    const parser = new Parser()
      .endianess(this.le)
      .uint8('isLeaf')
      .skip(1)
      .uint16('cnt')
      .choice({
        tag: 'isLeaf',
        choices: {
          1: new Parser().array('blocksToFetch', {
            length: 'cnt',
            type: new Parser()
              .uint32('startChrom')
              .uint32('startBase')
              .uint32('endChrom')
              .uint32('endBase')
              .buffer('blockOffset64', { length: 8 })
              .buffer('blockSize64', { length: 8 }),
          }),
          0: new Parser().array('recurOffsets', {
            length: 'cnt',
            type: new Parser()
              .uint32('startChrom')
              .uint32('startBase')
              .uint32('endChrom')
              .uint32('endBase')
              .buffer('blockOffset64', { length: 8 }),
          }),
        },
      })
    const p = parser.parse(data).result
    this.window.bwg.convert64Bits(p)

    const { chr, min, max } = this

    if (p.blocksToFetch) {
      this.blocksToFetch = p.blocksToFetch
        .filter(b => filterBlocks(b, chr, min, max))
        .map(l => ({ offset: l.blockOffset, size: l.blockSize }))
    }
    if (p.recurOffsets) {
      const recurOffsets = p.recurOffsets
        .filter(b => filterBlocks(b, chr, min, max))
        .map(l => l.blockOffset)
      if (recurOffsets.length > 0) {
        return this.cirFobRecur(recurOffsets, level + 1)
      }
    }
    return null
  }

  cirCompleted() {
    // merge contiguous blocks
    this.blockGroupsToFetch = RequestWorker.groupBlocks(this.blocksToFetch)

    if (this.blockGroupsToFetch.length === 0) {
      return []
    }
    return this.readFeatures()
  }

  static groupBlocks(blocks) {
    // sort the blocks by file offset
    blocks.sort((b0, b1) => (b0.offset | 0) - (b1.offset | 0))

    // group blocks that are within 2KB of eachother
    const blockGroups = []
    let lastBlock
    let lastBlockEnd
    for (let i = 0; i < blocks.length; i += 1) {
      if (lastBlock && blocks[i].offset - lastBlockEnd <= 2000) {
        lastBlock.size += blocks[i].size - lastBlockEnd + blocks[i].offset
        lastBlock.blocks.push(blocks[i])
      } else {
        blockGroups.push(
          (lastBlock = {
            blocks: [blocks[i]],
            size: blocks[i].size,
            offset: blocks[i].offset,
          }),
        )
      }
      lastBlockEnd = lastBlock.offset + lastBlock.size
    }

    return blockGroups
  }

  parseSummaryBlock(bytes, startOffset) {
    const data = bytes.slice(startOffset)
    const p = new Parser().endianess(this.le).array('summary', {
      length: data.byteLength / 64,
      type: new Parser()
        .int32('chromId')
        .int32('start')
        .int32('end')
        .int32('validCnt')
        .float('minVal')
        .float('maxVal')
        .float('sumData')
        .float('sumSqData'),
    })
    return p
      .parse(data)
      .result.summary.filter(elt => elt.chromId === this.chr)
      .map(elt => ({
        start: elt.start,
        end: elt.end,
        score: elt.sumData / elt.validCnt || 1,
        maxScore: elt.maxVal,
        minScore: elt.minVal,
        summary: true,
      }))
      .filter(f => this.coordFilter(f))
  }

  parseBigBedBlock(bytes, startOffset) {
    const data = bytes.slice(startOffset)
    const p = new Parser().endianess(this.le).array('items', {
      type: new Parser()
        .uint32('chromId')
        .int32('start')
        .int32('end')
        .string('rest', {
          zeroTerminated: true,
        }),
      readUntil: 'eof',
    })
    return p.parse(data).result.items.filter(f => this.coordFilter(f))
  }

  parseBigWigBlock(bytes, startOffset) {
    const data = bytes.slice(startOffset)
    const parser = new Parser()
      .endianess(this.le)
      .skip(4)
      .int32('blockStart')
      .skip(4)
      .uint32('itemStep')
      .uint32('itemSpan')
      .uint8('blockType')
      .skip(1)
      .uint16('itemCount')
      .choice({
        tag: 'blockType',
        choices: {
          [BIG_WIG_TYPE_FSTEP]: new Parser().array('items', {
            length: 'itemCount',
            type: new Parser().float('score'),
          }),
          [BIG_WIG_TYPE_VSTEP]: new Parser().array('items', {
            length: 'itemCount',
            type: new Parser().int32('start').float('score'),
          }),
          [BIG_WIG_TYPE_GRAPH]: new Parser().array('items', {
            length: 'itemCount',
            type: new Parser()
              .int32('start')
              .int32('end')
              .float('score'),
          }),
        },
      })
    const results = parser.parse(data).result
    let items = results.items
    if (results.blockType === BIG_WIG_TYPE_FSTEP) {
      const { itemStep: step } = results
      items = items.map((s, i) => ({
        ...s,
        start: i * step,
        end: i * step + step,
      }))
    } else if (results.blockType === BIG_WIG_TYPE_VSTEP) {
      for (let i = 0; i < items.length - 1; i += 1) {
        items[i].end = items[i + 1].start - 1
      }
    }
    return items.filter(f => this.coordFilter(f))
  }

  coordFilter(f) {
    return f.start < this.max && f.end >= this.min
  }

  /* eslint no-param-reassign: ["error", { "props": false }] */
  readFeatures() {
    return this.blockGroupsToFetch.map(blockGroup => {
      const recvdata = Buffer.alloc(blockGroup.size)
      return this.window.bwg.bbi
        .read(recvdata, 0, blockGroup.size, blockGroup.offset)
        .then(() =>
          blockGroup.blocks.map(block => {
            let data
            let offset = block.offset - blockGroup.offset

            if (this.window.bwg.header.uncompressBufSize > 0) {
              data = zlib.inflateSync(recvdata.slice(offset))
              offset = 0
            } else {
              data = recvdata
            }

            if (this.window.isSummary) {
              return this.parseSummaryBlock(data, offset)
            }
            if (this.window.bwg.type === 'bigwig') {
              return this.parseBigWigBlock(data, offset)
            }
            if (this.window.bwg.type === 'bigbed') {
              return this.parseBigBedBlock(data, offset)
            }
            console.warn(`Don't know what to do with ${this.window.bwg.type}`)
            return undefined
          }),
        )
        .then(res => res.flat())
    })
  }
}
