/* eslint no-bitwise: ["error", { "allow": ["|"] }] */
export class AbortError extends Error {
  public code: string

  public constructor(message: string) {
    super(message)
    this.code = 'ERR_ABORTED'
  }
}

export interface Block {
  offset: number
  length: number
}
export interface BlockGroup {
  offset: number
  length: number
  blocks: Block[]
}
// sort blocks by file offset and
// group blocks that are within 2KB of eachother
export function groupBlocks(blocks: Block[]): BlockGroup[] {
  blocks.sort((b0, b1) => (b0.offset | 0) - (b1.offset | 0))

  const blockGroups = []
  let lastBlock
  let lastBlockEnd = 0
  for (let i = 0; i < blocks.length; i += 1) {
    if (lastBlock && blocks[i].offset - lastBlockEnd <= 2000) {
      lastBlock.length += blocks[i].length - lastBlockEnd + blocks[i].offset
      lastBlock.blocks.push(blocks[i])
    } else {
      blockGroups.push(
        (lastBlock = {
          blocks: [blocks[i]],
          length: blocks[i].length,
          offset: blocks[i].offset,
        }),
      )
    }
    lastBlockEnd = lastBlock.offset + lastBlock.length
  }

  return blockGroups
}

/**
 * Properly check if the given AbortSignal is aborted.
 * Per the standard, if the signal reads as aborted,
 * this function throws either a DOMException AbortError, or a regular error
 * with a `code` attribute set to `ERR_ABORTED`.
 *
 * For convenience, passing `undefined` is a no-op
 *
 * @param {AbortSignal} [signal] an AbortSignal, or anything with an `aborted` attribute
 * @returns nothing
 */
export function checkAbortSignal(signal?: AbortSignal): void {
  if (!signal) return

  if (signal.aborted) {
    if (typeof DOMException !== 'undefined') {
      throw new DOMException('aborted', 'AbortError')
    } else {
      const e = new AbortError('aborted')
      e.code = 'ERR_ABORTED'
      throw e
    }
  }
}

/**
 * Skips to the next tick, then runs `checkAbortSignal`.
 * Await this to inside an otherwise synchronous loop to
 * provide a place to break when an abort signal is received.
 * @param {AbortSignal} signal
 */
export async function abortBreakPoint(signal?: AbortSignal): Promise<void> {
  await Promise.resolve()
  checkAbortSignal(signal)
}

type AbortableCallback<T> = (signal: AbortSignal) => Promise<T>

/* A class that provides memoization for abortable calls */
export class AbortAwareCache<T> {
  private cache: Map<AbortableCallback<T>, Promise<T>> = new Map()

  /*
   * Takes a function that has one argument, abortSignal, that returns a promise
   * and it works by retrying the function if a previous attempt to initialize the parse cache was aborted
   * @param fn - an AbortableCallback
   * @return a memoized version of the AbortableCallback using the AbortAwareCache
   */
  public abortableMemoize(fn: (signal?: AbortSignal) => Promise<T>): (signal?: AbortSignal) => Promise<T> {
    const { cache } = this
    return function abortableMemoizeFn(signal?: AbortSignal) {
      if (!cache.has(fn)) {
        const fnReturn = fn(signal)
        cache.set(fn, fnReturn)
        if (signal) {
          fnReturn.catch((): void => {
            if (signal.aborted) cache.delete(fn)
          })
        }
      }
      const ret = cache.get(fn)
      if (!ret) {
        throw new Error('abortable memoization function not found')
      }
      return ret.catch(
        (e: AbortError | DOMException): Promise<T> => {
          if (e.code === 'ERR_ABORTED' || e.name === 'AbortError') {
            return fn(signal)
          }
          throw e
        },
      )
    }
  }
}
