import { createHash } from 'crypto'
import { readFileSync } from 'fs'

export type Ref =
  | {
      type: 'raw'
      buffer: Buffer
    }
  | {
      type: 'combine'
      left: Ref
      right: Ref
    }
  | { type: 'hash'; hash: Buffer }

const hashFn = 'sha256'

export function hashBuffer(buffer: Buffer) {
  const hash = createHash(hashFn)
  hash.write(buffer)
  return hash.digest()
}

export function store(file: string) {
  const bin = readFileSync(file)

  const mem: Record<string, Ref> = {}

  function getRef(buffer: Buffer): Ref {
    const hash = hashBuffer(buffer)
    const hex = hash.toString('hex')
    if (hex in mem) {
      return { type: 'hash', hash }
    }
    const ref: Ref = {} as any
    mem[hex] = ref

    const hashLength = 32
    const minLength = hashLength * 4
    if (buffer.length < minLength) {
      Object.assign(ref, {
        type: 'raw',
        buffer,
      })
      return ref
    }
    const midIndex = Math.floor(buffer.length / 2)
    Object.assign(ref, {
      type: 'combine',
      left: getRef(buffer.slice(0, midIndex)),
      right: getRef(buffer.slice(midIndex)),
    })
    return ref
  }

  const root = getRef(bin)

  let numRef = 0
  let rawSize = 0
  let numRaw = 0
  Object.values(mem).forEach(ref => {
    numRef++
    if (ref.type === 'raw') {
      numRaw++
      rawSize += ref.buffer.length
    }
  })
  console.debug({ numRef, numRaw, rawSize, root })
}

store('raw')
