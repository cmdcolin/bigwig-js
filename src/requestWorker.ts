/* eslint no-bitwise: ["error", { "allow": ["|"] }] */
const { Parser } = require('@gmod/binary-parser')
const zlib = require('zlib')
import Range from './range'
import LocalFile from './localFile'
import {convert64Bits,groupBlocks} from './util'
const BIG_WIG_TYPE_GRAPH = 1
const BIG_WIG_TYPE_VSTEP = 2
const BIG_WIG_TYPE_FSTEP = 3

interface Feature {
  start: number
  end: number
  score: number
  minScore?: number
  maxScore?: number
  summary?: boolean
}

interface DataBlock {
  startChrom: number,
  endChrom: number,
  startBase: number,
  endBase: number,
  validCnt: number,
  minVal: number,
  maxVal: number,
  sumData: number,
  sumSqData: number
}

interface SummaryBlock {
  chromId: number,
  startBase: number,
  endBase: number,
  validCnt: number,
  minVal: number,
  maxVal: number,
  sumData: number,
  sumSqData: number
}
interface Options {
  type: string,
  compressed: boolean,
  isBigEndian: boolean,
  cirBlockSize: number,
  name?: string
}
/**
 * Worker object for reading data from a bigwig or bigbed file.
 * Manages the state necessary for traversing the index trees and
 * so forth.
 *
 * Adapted by Robert Buels from bigwig.js in the Dalliance Genome
 * Explorer by Thomas Down.
 * @constructs
 */
export default class RequestWorker {
  private window: any
  private source: string|undefined
  private le: string
  private blocksToFetch: any[]
  private outstanding: number
  private chrId: number
  private min: number
  private max: number
  private data: LocalFile
  private cirBlockSize: number
  private type: string
  private compressed: boolean
  private isBigEndian: boolean

  constructor(data: LocalFile, chrId:number, min:number, max:number, opts: Options) {
    this.source = opts.name
    this.cirBlockSize = opts.cirBlockSize
    this.compressed = opts.compressed
    this.type = opts.type
    this.isBigEndian = opts.isBigEndian
    this.le = opts.isBigEndian ? 'big' : 'little'
    this.data = data

    this.blocksToFetch = []
    this.outstanding = 0

    this.chrId = chrId
    this.min = min
    this.max = max
  }

  cirFobRecur(offset: any, level: number) {
    this.outstanding += offset.length

    const maxCirBlockSpan = 4 + this.cirBlockSize * 32 // Upper bound on size, based on a completely full leaf node.
    let spans
    for (let i = 0; i < offset.length; i += 1) {
      const blockSpan = new Range(offset[i], offset[i] + maxCirBlockSpan)
      spans = spans ? spans.union(blockSpan) : blockSpan
    }

    return spans.getRanges().map((fr: any) => this.cirFobStartFetch(offset, fr, level))
  }

  async cirFobStartFetch(offset: any, fr: any, level: number) {
    const length = fr.max() - fr.min()
    const resultBuffer = Buffer.alloc(length)
    await this.data.read(resultBuffer, 0, length, fr.min())
    return new Promise((resolve, reject) => {
      for (let i = 0; i < offset.length; i += 1) {
        if (fr.contains(offset[i])) {
          this.cirFobRecur2(resultBuffer, offset[i] - fr.min(), level)
          this.outstanding -= 1
          if (this.outstanding === 0) {
            resolve(this.readFeatures())
          }
        }
      }
      if (this.outstanding !== 0) {
        reject(new Error('did not complete'))
      }
    })
  }

  cirFobRecur2(cirBlockData: Buffer, offset: number, level: number) {
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
    convert64Bits(p,this.isBigEndian)

    // prettier-ignore
    const m = (block:DataBlock):boolean =>
      (block.startChrom < this.chrId || (block.startChrom === this.chrId && block.startBase <= this.max)) &&
      (block.endChrom > this.chrId || (block.endChrom === this.chrId && block.endBase >= this.min))

    if (p.blocksToFetch) {
      this.blocksToFetch = p.blocksToFetch.filter(m).map((l: any):any => ({ offset: l.blockOffset, size: l.blockSize }))
    }
    if (p.recurOffsets) {
      const recurOffsets = p.recurOffsets.filter(m).map((l:any):any => l.blockOffset)
      if (recurOffsets.length > 0) {
        return this.cirFobRecur(recurOffsets, level + 1)
      }
    }
    return null
  }

  parseSummaryBlock(bytes: Buffer, startOffset: number) {
    const data = bytes.slice(startOffset)
    const p = new Parser().endianess(this.le).array('summary', {
      length: data.byteLength / 64,
      type: new Parser()
        .int32('chromId')
        .int32('startBase')
        .int32('endBase')
        .int32('validCnt')
        .float('minVal')
        .float('maxVal')
        .float('sumData')
        .float('sumSqData'),
    })
    return p
      .parse(data)
      .result.summary.filter((elt: SummaryBlock):boolean => elt.chromId === this.chrId)
      .map((elt:SummaryBlock):Feature => ({
        start: elt.startBase,
        end: elt.endBase,
        score: elt.sumData / elt.validCnt || 1,
        maxScore: elt.maxVal,
        minScore: elt.minVal,
        summary: true,
      }))
      .filter((f:Feature):boolean => this.coordFilter(f))
  }

  parseBigBedBlock(bytes: Buffer, startOffset: number) {
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
    return p.parse(data).result.items.filter((f: any) => this.coordFilter(f))
  }

  parseBigWigBlock(bytes: Buffer, startOffset: number) {
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
      items = items.map((s: any, i: number) => ({
        ...s,
        start: i * step,
        end: i * step + step,
      }))
    } else if (results.blockType === BIG_WIG_TYPE_VSTEP) {
      for (let i = 0; i < items.length - 1; i += 1) {
        items[i].end = items[i + 1].start - 1
      }
    }
    return items.filter((f: any) => this.coordFilter(f))
  }

  coordFilter(f: Feature):boolean {
    return f.start < this.max && f.end >= this.min
  }

  async readFeatures() {
    const blockGroupsToFetch = groupBlocks(this.blocksToFetch)
    const blockFetches = blockGroupsToFetch.map((blockGroup: any) => {
      const data = Buffer.alloc(blockGroup.size)
      return this.data.read(data, 0, blockGroup.size, blockGroup.offset).then(() => {
        blockGroup.data = data
        return blockGroup
      })
    })

    const blockGroups = await Promise.all(blockFetches)
    const ret = blockGroups.map((blockGroup: any) =>
      blockGroup.blocks.map((block: any) => {
        let data
        let offset = block.offset - blockGroup.offset

        if (this.compressed) {
          data = zlib.inflateSync(blockGroup.data.slice(offset))
          offset = 0
        } else {
          // eslint-disable-next-line
          data = blockGroup.data
        }

        switch(this.type) {
          case 'summary':
          return this.parseSummaryBlock(data, offset)
          case 'bigwig':
          return this.parseBigWigBlock(data, offset)
          case 'bigbed':
          return this.parseBigBedBlock(data, offset)
          default:
          console.warn(`Don't know what to do with ${this.type}`)
          return undefined
        }
      }),
    )
    const flatten = (list: any) => list.reduce((a: any, b: any) => a.concat(Array.isArray(b) ? flatten(b) : b), [])
    return flatten(ret)
  }
}
