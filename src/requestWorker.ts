/* eslint no-bitwise: ["error", { "allow": ["|"] }] */
import { Parser } from '@gmod/binary-parser'
import * as Long from 'long'
import * as zlib from 'zlib'
import Range from './range'
import LocalFile from './localFile'
import { groupBlocks } from './util'
import Feature from './feature'
import { Observer } from 'rxjs'

const BIG_WIG_TYPE_GRAPH = 1
const BIG_WIG_TYPE_VSTEP = 2
const BIG_WIG_TYPE_FSTEP = 3

interface DataBlock {
  startChrom: number
  endChrom: number
  startBase: number
  endBase: number
  validCnt: number
  minVal: number
  maxVal: number
  sumData: number
  sumSqData: number
}

interface SummaryBlock {
  chromId: number
  startBase: number
  endBase: number
  validCnt: number
  minVal: number
  maxVal: number
  sumData: number
  sumSqData: number
}
interface Options {
  type: string
  isCompressed: boolean
  isBigEndian: boolean
  cirBlockSize: number
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
  private blocksToFetch: any[]
  private outstanding: number
  private chrId: number
  private min: number
  private max: number
  private data: LocalFile
  private opts: Options
  private observer: Observer<Feature[]>

  public constructor(
    data: LocalFile,
    chrId: number,
    min: number,
    max: number,
    observer: Observer<Feature[]>,
    opts: Options,
  ) {
    Object.assign(this, opts)
    this.opts = opts
    this.data = data
    this.observer = observer

    this.blocksToFetch = []
    this.outstanding = 0

    this.chrId = chrId
    this.min = min
    this.max = max
  }

  public cirFobRecur(offset: any, level: number) {
    this.outstanding += offset.length

    const maxCirBlockSpan = 4 + this.opts.cirBlockSize * 32 // Upper bound on size, based on a completely full leaf node.
    let spans = new Range(offset[0], offset[0] + maxCirBlockSpan)
    for (let i = 1; i < offset.length; i += 1) {
      const blockSpan = new Range(offset[i], offset[i] + maxCirBlockSpan)
      spans = spans.union(blockSpan)
    }
    spans.getRanges().map((fr: Range) => this.cirFobStartFetch(offset, fr, level))
  }

  private async cirFobStartFetch(offset: any, fr: any, level: number) {
    const length = fr.max() - fr.min()
    const resultBuffer = Buffer.alloc(length)
    await this.data.read(resultBuffer, 0, length, fr.min())
    for (let i = 0; i < offset.length; i += 1) {
      if (fr.contains(offset[i])) {
        this.cirFobRecur2(resultBuffer, offset[i] - fr.min(), level)
        this.outstanding -= 1
        if (this.outstanding === 0) {
          this.readFeatures()
        }
      }
    }
    if (this.outstanding !== 0) {
      new Error('did not complete')
    }
  }

  private cirFobRecur2(cirBlockData: Buffer, offset: number, level: number) {
    const data = cirBlockData.slice(offset)

    /* istanbul ignore next */
    const parser = new Parser()
      .endianess(this.opts.isBigEndian ? 'big' : 'little')
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
              .buffer('blockOffset', {
                length: 8,
                formatter: function(buf: any): number {
                  return Long.fromBytes(buf, true, this.endian === 'le').toNumber()
                },
              })
              .buffer('blockSize', {
                length: 8,
                formatter: function(buf: any): number {
                  return Long.fromBytes(buf, true, this.endian === 'le').toNumber()
                },
              }),
          }),
          0: new Parser().array('recurOffsets', {
            length: 'cnt',
            type: new Parser()
              .uint32('startChrom')
              .uint32('startBase')
              .uint32('endChrom')
              .uint32('endBase')
              .buffer('blockOffset', {
                length: 8,
                formatter: function(buf: any): number {
                  return Long.fromBytes(buf, true, this.endian === 'le').toNumber()
                },
              }),
          }),
        },
      })
    const p = parser.parse(data).result
    const { chrId, max, min } = this

    const m = (b: DataBlock): boolean =>
      (b.startChrom < chrId || (b.startChrom === chrId && b.startBase <= max)) &&
      (b.endChrom > chrId || (b.endChrom === chrId && b.endBase >= min))

    if (p.blocksToFetch) {
      this.blocksToFetch = p.blocksToFetch
        .filter(m)
        .map((l: any): any => ({ offset: l.blockOffset, size: l.blockSize }))
    }
    if (p.recurOffsets) {
      const recurOffsets = p.recurOffsets.filter(m).map((l: any): any => l.blockOffset)
      if (recurOffsets.length > 0) {
        return this.cirFobRecur(recurOffsets, level + 1)
      }
    }
    return null
  }

  private parseSummaryBlock(bytes: Buffer, startOffset: number) {
    const data = bytes.slice(startOffset)
    const p = new Parser()
      .endianess(this.opts.isBigEndian ? 'big' : 'little')
      .array('summary', {
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
      .result.summary.filter((elt: SummaryBlock): boolean => elt.chromId === this.chrId)
      .map(
        (elt: SummaryBlock): Feature => ({
          start: elt.startBase,
          end: elt.endBase,
          score: elt.sumData / elt.validCnt || 1,
          maxScore: elt.maxVal,
          minScore: elt.minVal,
          summary: true,
        }),
      )
      .filter((f: Feature): boolean => this.coordFilter(f))
  }

  private parseBigBedBlock(bytes: Buffer, startOffset: number) {
    const data = bytes.slice(startOffset)
    const p = new Parser()
      .endianess(this.opts.isBigEndian ? 'big' : 'little')
      .array('items', {
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

  private parseBigWigBlock(bytes: Buffer, startOffset: number) {
    const data = bytes.slice(startOffset)
    const parser = new Parser()
      .endianess(this.opts.isBigEndian ? 'big' : 'little')
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

  private coordFilter(f: Feature): boolean {
    return f.start < this.max && f.end >= this.min
  }

  private async readFeatures() {
    const blockGroupsToFetch = groupBlocks(this.blocksToFetch)
    await Promise.all(blockGroupsToFetch.map(async (blockGroup: any) => {
      let data = Buffer.alloc(blockGroup.size)
      await this.data.read(data, 0, blockGroup.size, blockGroup.offset)
      blockGroup.blocks.forEach((block: any) => {
        let offset = block.offset - blockGroup.offset
        data = this.opts.isCompressed ? zlib.inflateSync(data.slice(offset)) : data
        offset = this.opts.isCompressed ? 0 : offset

        switch (this.opts.type) {
          case 'summary':
            this.observer.next(this.parseSummaryBlock(data, offset))
            break
          case 'bigwig':
            this.observer.next(this.parseBigWigBlock(data, offset))
            break
          case 'bigbed':
            this.observer.next(this.parseBigBedBlock(data, offset))
            break
          default:
            console.warn(`Don't know what to do with ${this.opts.type}`)
        }
      })
    }))
    console.log('here')
    this.observer.complete()
  }
}
