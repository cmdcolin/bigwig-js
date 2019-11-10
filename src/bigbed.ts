import { Parser } from '@gmod/binary-parser'
import { Observable, Observer, merge } from 'rxjs'
import { map, reduce } from 'rxjs/operators'
import { AbortAwareCache } from './util'

import { BBI, BigBedFeature } from './bbi'

interface Loc {
  key: string
  offset: number
  length: number
  field: number
}

interface SearchOptions {
  signal?: AbortSignal
}

interface Index {
  type: number
  fieldcount: number
  offset: number
  field: number
}

export function filterUndef<T>(ts: (T | undefined)[]): T[] {
  return ts.filter((t: T | undefined): t is T => !!t)
}

export class BigBed extends BBI {
  protected indicesCache: AbortAwareCache<Index[]> = new AbortAwareCache()
  public readIndices: (abortSignal?: AbortSignal) => Promise<Index[]> = this.indicesCache.abortableMemoize(
    this._readIndices.bind(this),
  )

  /*
   * retrieve unzoomed view for any scale
   * @param scale - unused
   * @param abortSignal - an optional AbortSignal to kill operation
   * @return promise for a BlockView
   */
  protected async getView(scale: number, abortSignal: AbortSignal) {
    return this.getUnzoomedView(abortSignal)
  }

  /*
   * parse the bigbed extraIndex fields
   * @param abortSignal to abort operation
   * @return a Promise for an array of Index data structure since there can be multiple extraIndexes in a bigbed, see bedToBigBed documentation
   */
  public async _readIndices(abortSignal?: AbortSignal) {
    const { extHeaderOffset, isBigEndian } = await this.getHeader(abortSignal)
    const { buffer: data } = await this.bbi.read(Buffer.alloc(64), 0, 64, extHeaderOffset)
    const le = isBigEndian ? 'big' : 'little'
    const ret = new Parser()
      .endianess(le)
      .uint16('size')
      .uint16('count')
      .uint64('offset')
      .parse(data).result
    const { count, offset } = ret

    // no extra index is defined if count==0
    if (count === 0) {
      return []
    }

    const blocklen = 20
    const len = blocklen * count
    const { buffer } = await this.bbi.read(Buffer.alloc(len), 0, len, offset)
    const extParser = new Parser()
      .endianess(le)
      .int16('type')
      .int16('fieldcount')
      .uint64('offset')
      .skip(4)
      .int16('field')
    const indices = []

    for (let i = 0; i < count; i += 1) {
      indices.push(extParser.parse(buffer.slice(i * blocklen)).result)
    }
    return indices
  }

  /*
   * perform a search in the bigbed extraIndex to find which blocks in the bigbed data to look for the
   * actual feature data
   *
   * @param name - the name to search for
   * @param opts - a SearchOptions argument with optional signal
   * @return a Promise for an array of bigbed block Loc entries
   */
  private async searchExtraIndexBlocks(name: string, opts: SearchOptions = {}) {
    const { signal } = opts
    const { isBigEndian } = await this.getHeader(signal)
    const indices = await this.readIndices(signal)
    if (!indices.length) {
      return []
    }
    const locs = indices.map(
      async (index): Promise<Loc | undefined> => {
        const { offset, field } = index
        const { buffer: data } = await this.bbi.read(Buffer.alloc(32), 0, 32, offset, { signal })
        const p = new Parser()
          .endianess(isBigEndian ? 'big' : 'little')
          .int32('magic')
          .int32('blockSize')
          .int32('keySize')
          .int32('valSize')
          .uint64('itemCount')

        const { blockSize, keySize, valSize } = p.parse(data).result
        const bpt = new Parser()
          .endianess(isBigEndian ? 'big' : 'little')
          .int8('nodeType')
          .skip(1)
          .int16('cnt')
          .choice({
            tag: 'nodeType',
            choices: {
              0: new Parser().array('leafkeys', {
                length: 'cnt',
                type: new Parser().string('key', { length: keySize, stripNull: true }).uint64('offset'),
              }),
              1: new Parser().array('keys', {
                length: 'cnt',
                type: new Parser()
                  .string('key', { length: keySize, stripNull: true })
                  .uint64('offset')
                  .uint32('length')
                  .uint32('reserved'),
              }),
            },
          })

        const bptReadNode = async (nodeOffset: number): Promise<Loc | undefined> => {
          const len = 4 + blockSize * (keySize + valSize)
          const { buffer } = await this.bbi.read(Buffer.alloc(len), 0, len, nodeOffset, { signal })
          const node = bpt.parse(buffer).result
          if (node.leafkeys) {
            let lastOffset
            for (let i = 0; i < node.leafkeys.length; i += 1) {
              const { key } = node.leafkeys[i]
              if (name.localeCompare(key) < 0 && lastOffset) {
                return bptReadNode(lastOffset)
              }
              lastOffset = node.leafkeys[i].offset
            }
            return bptReadNode(lastOffset)
          }
          for (let i = 0; i < node.keys.length; i += 1) {
            if (node.keys[i].key === name) {
              return { ...node.keys[i], field }
            }
          }

          return undefined
        }
        const rootNodeOffset = 32
        return bptReadNode(offset + rootNodeOffset)
      },
    )
    return filterUndef(await Promise.all(locs))
  }

  /*
   * retrieve the features from the bigbed data that were found through the lookup of the extraIndex
   * note that there can be multiple extraIndex, see the BigBed specification and the -extraIndex argument to bedToBigBed
   *
   * @param name - the name to search for
   * @param opts - a SearchOptions argument with optional signal
   * @return a Promise for an array of Feature
   */
  public async searchExtraIndex(name: string, opts: SearchOptions = {}) {
    const blocks = await this.searchExtraIndexBlocks(name, opts)
    if (!blocks.length) return []
    const view = await this.getUnzoomedView()
    const res = blocks.map(block => {
      return new Observable((observer: Observer<unknown[]>) => {
        view.readFeatures(observer, [block], opts)
      }).pipe(
        reduce((acc, curr) => acc.concat(curr)),
        map(
          (x: unknown[]): BigBedFeature => {
            for (let i = 0; i < x.length; i += 1) {
              ;(x[i] as BigBedFeature).field = block.field
            }
            // @ts-ignore
            return x as BigBedFeature[]
          },
        ),
      )
    })
    // @ts-ignore
    const ret: BigBedFeature[] = await merge(...res).toPromise()
    return ret.filter(f => {
      return f.rest.split('\t')[f.field - 3] === name
    })
  }
}
