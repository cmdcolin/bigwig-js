import * as Long from 'long';

// mutates obj for keys ending with '64' to longs, and removes the '64' suffix
/* eslint no-param-reassign: ["error", { "props": false }] */
export function convert64Bits(obj: any, isBigEndian: boolean): any {
  const keys = Object.keys(obj)
  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i]
    const val = obj[key]
    if (key.endsWith('64')) {
      obj[key.slice(0, -2)] = Long.fromBytes(val, false, !isBigEndian).toNumber()
      delete obj[key]
    } else if (typeof obj[key] === 'object' && val !== null) {
      convert64Bits(val,isBigEndian)
    }
  }
}

// sort blocks by file offset and
// group blocks that are within 2KB of eachother
export function groupBlocks(blocks: Array<any>): Array<any> {
    blocks.sort((b0, b1) => (b0.offset | 0) - (b1.offset | 0))

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
